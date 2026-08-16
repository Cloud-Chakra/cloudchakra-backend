const crypto = require('crypto');
const WebSocket = require('ws');
const { v4: uuidv4 } = require('uuid');
const { Phone, Document } = require('./models');
const config = require('./config');

// ---------- WebSocket Manager ----------
class WebSocketManager {
  constructor() {
    this.connections = new Map();
    this.pendingRequests = new Map();
    this.heartbeatInterval = null;
    this.replicationInterval = null;
  }

  addConnection(deviceId, ws) {
    if (!deviceId || typeof deviceId !== 'string' || deviceId.trim() === '') {
      console.error('[WebSocketManager] Rejecting connection with invalid deviceId:', deviceId);
      return;
    }

    if (this.connections.has(deviceId)) {
      const current = this.connections.get(deviceId);
      if (current === ws) return;
      console.log('[WebSocketManager] Replacing existing connection for', deviceId);
      current.terminate();
    }

    this.connections.set(deviceId, ws);
    console.log('[WebSocketManager] Connection added. Online count:', this.getOnlinePhones().length);

    Phone.findOneAndUpdate(
      { deviceId },
      { status: 'online', lastSeen: new Date() },
      { upsert: true }
    ).catch(err => console.error('Error updating phone status:', err));

    // Trigger replication for under-replicated documents
    storageService.handlePhoneOnline(deviceId).catch(err =>
      console.error(`Error handling phone online for ${deviceId}:`, err)
    );
  }

  removeConnection(deviceId, ws) {
    if (!deviceId || typeof deviceId !== 'string') return;

    const current = this.connections.get(deviceId);
    if (current && current !== ws) {
      console.log(`[WebSocketManager] Ignoring close of old connection for ${deviceId}`);
      return;
    }

    this.connections.delete(deviceId);
    console.log('[WebSocketManager] Connection removed. Online count:', this.getOnlinePhones().length);

    Phone.findOneAndUpdate(
      { deviceId },
      { status: 'offline' }
    ).catch(err => console.error('Error updating phone status:', err));

    // No complex offline handling needed
  }

  isOnline(deviceId) {
    if (!deviceId || typeof deviceId !== 'string') return false;
    const ws = this.connections.get(deviceId);
    return ws && ws.readyState === WebSocket.OPEN;
  }

  getOnlinePhones() {
    const online = [];
    for (const [deviceId, ws] of this.connections.entries()) {
      if (deviceId && typeof deviceId === 'string' && deviceId.trim() !== '' && ws.readyState === WebSocket.OPEN) {
        online.push(deviceId);
      } else {
        console.warn('[WebSocketManager] Skipping invalid/offline connection:', deviceId, ws?.readyState);
      }
    }
    return online;
  }

  sendRequest(deviceId, type, payload, timeout = config.requestTimeout || 15000) {
    return new Promise((resolve, reject) => {
      if (!deviceId || typeof deviceId !== 'string' || deviceId.trim() === '') {
        return reject(new Error('Device ID is missing or invalid'));
      }

      const ws = this.connections.get(deviceId);
      if (!ws || ws.readyState !== WebSocket.OPEN) {
        return reject(new Error(`Phone ${deviceId} is not online`));
      }

      const requestId = crypto.randomBytes(16).toString('hex');
      const message = { type, requestId, ...payload };

      const timeoutId = setTimeout(() => {
        this.pendingRequests.delete(requestId);
        reject(new Error(`Timeout waiting for response from ${deviceId}`));
      }, timeout);

      this.pendingRequests.set(requestId, { resolve, reject, type, timeoutId });
      ws.send(JSON.stringify(message));
    });
  }

  handleResponse(deviceId, message) {
    const { requestId, success, error, data } = message;
    const pending = this.pendingRequests.get(requestId);

    if (!pending) return;

    clearTimeout(pending.timeoutId);
    this.pendingRequests.delete(requestId);

    if (success) {
      pending.resolve(data || {});
    } else {
      pending.reject(new Error(error || 'Unknown error'));
    }
  }

  startHeartbeat(interval = 30000) {
    if (this.heartbeatInterval) clearInterval(this.heartbeatInterval);

    this.heartbeatInterval = setInterval(async () => {
      for (const [deviceId, ws] of this.connections.entries()) {
        if (ws.readyState === WebSocket.OPEN) ws.ping();
      }

      // Mark phones offline if they are not actually connected
      const allPhones = await Phone.find({ status: 'online' });
      for (const phone of allPhones) {
        if (!this.isOnline(phone.deviceId)) {
          phone.status = 'offline';
          await phone.save();
        }
      }
    }, interval);
  }

  startReplicationSweep(interval = 60000) {
    if (this.replicationInterval) clearInterval(this.replicationInterval);

    this.replicationInterval = setInterval(async () => {
      try {
        await storageService.replicationSweep();
      } catch (err) {
        console.error('Replication sweep error:', err);
      }
    }, interval);
  }
}

// ---------- Global lock to prevent concurrent replication of the same doc ----------
const replicatingDocs = new Set();

// ---------- Storage Service (Simplified Replication) ----------
const storageService = {
  /**
   * Store a document on at least one online phone and return its ID.
   * Replication to additional devices (up to 3 total) is done asynchronously.
   */
  async storeData(jsonObj) {
    const jsonStr = JSON.stringify(jsonObj);
    const docId = uuidv4();

    const onlinePhones = webSocketManager.getOnlinePhones();
    if (onlinePhones.length === 0) {
      throw new Error('No online phones available to store data');
    }

    const firstDevice = onlinePhones[0];

    // Store on first available device synchronously
    await webSocketManager.sendRequest(firstDevice, 'STORE_DOC', {
      docId,
      data: jsonStr
    });

    // Create document record with the first device in deviceIds
    const doc = new Document({
      docId,
      deviceIds: [firstDevice]
    });
    await doc.save();

    // Asynchronously try to replicate to up to 2 more devices
    setImmediate(() => {
      storageService.ensureReplication(doc).catch(err => {
        console.error(`Initial replication for doc ${docId} failed:`, err);
      });
    });

    return docId;
  },

  /**
   * Retrieve a document by trying any device in its deviceIds array.
   * Devices are tried in random order to balance load.
   */
  async retrieveData(docId) {
    const doc = await Document.findOne({ docId });
    if (!doc) throw new Error('Document not found');

    const deviceIds = doc.deviceIds || [];
    if (deviceIds.length === 0) throw new Error('No devices recorded for this document');

    // Shuffle to pick a random device
    const shuffled = [...deviceIds].sort(() => Math.random() - 0.5);

    for (const deviceId of shuffled) {
      if (!webSocketManager.isOnline(deviceId)) continue;

      try {
        const result = await webSocketManager.sendRequest(deviceId, 'RETRIEVE_DOC', { docId });
        const data = result.data;
        if (typeof data === 'string' && data.length > 0 && data !== 'undefined') {
          return JSON.parse(data);
        }
      } catch (err) {
        console.warn(`Failed to retrieve from device ${deviceId}: ${err.message}`);
      }
    }

    throw new Error('No online copy available for this document');
  },

  /**
   * Called when a phone comes online.
   * Finds documents with fewer than 3 deviceIds that do not already contain this device,
   * and replicates the document to this device if possible.
   */
  async handlePhoneOnline(deviceId) {
    const docs = await Document.find({
      $expr: { $lt: [{ $size: '$deviceIds' }, 3] },
      deviceIds: { $nin: [deviceId] }
    });

    const concurrency = 5;
    for (let i = 0; i < docs.length; i += concurrency) {
      const batch = docs.slice(i, i + concurrency);
      await Promise.all(batch.map(doc => storageService.ensureReplication(doc)));
    }
  },

  /**
   * Called when a phone goes offline.
   * We do nothing here – the device remains in deviceIds, but retrieval will skip it.
   */
  async handlePhoneOffline(deviceId) {
    // No action needed
  },

  /**
   * Ensure a document has at least 3 devices in its deviceIds array.
   * If fewer than 3, tries to retrieve data from an online device that already has the copy
   * and sends it to new online devices not already in the array.
   */
  async ensureReplication(doc) {
    if (replicatingDocs.has(doc.docId)) {
      console.log(`Replication already in progress for doc ${doc.docId}, skipping.`);
      return;
    }
    replicatingDocs.add(doc.docId);

    try {
      // Normalize deviceIds: ensure array, remove duplicates
      if (!Array.isArray(doc.deviceIds)) {
        doc.deviceIds = [];
      }
      doc.deviceIds = [...new Set(doc.deviceIds)];

      // If we already have 3 devices, nothing to do
      if (doc.deviceIds.length >= 3) {
        await doc.save();
        return;
      }

      // Find online devices not already in deviceIds
      const onlinePhones = webSocketManager.getOnlinePhones();
      const candidates = onlinePhones.filter(id => !doc.deviceIds.includes(id));

      if (candidates.length === 0) {
        // No new devices available
        await doc.save();
        return;
      }

      // Need a source device that is online and already has the copy
      const sourceDevice = doc.deviceIds.find(id => webSocketManager.isOnline(id));
      if (!sourceDevice) {
        console.warn(`Cannot replicate doc ${doc.docId}: no online copy available to source data`);
        await doc.save();
        return;
      }

      // Retrieve data from source
      let dataStr;
      try {
        const result = await webSocketManager.sendRequest(sourceDevice, 'RETRIEVE_DOC', { docId: doc.docId });
        dataStr = result.data;
        if (typeof dataStr !== 'string' || dataStr.length === 0 || dataStr === 'undefined') {
          throw new Error('Invalid data from source');
        }
      } catch (err) {
        console.error(`Failed to retrieve from source for replication: ${err.message}`);
        await doc.save();
        return;
      }

      // Determine how many new devices we can add
      const needed = Math.min(candidates.length, 3 - doc.deviceIds.length);
      const devicesToAdd = candidates.slice(0, needed);

      for (const newDevice of devicesToAdd) {
        try {
          await webSocketManager.sendRequest(newDevice, 'STORE_DOC', {
            docId: doc.docId,
            data: dataStr
          });
          doc.deviceIds.push(newDevice);
          console.log(`Replicated doc ${doc.docId} to ${newDevice}`);
        } catch (err) {
          console.error(`Failed to replicate to ${newDevice}: ${err.message}`);
        }
      }

      await doc.save();
    } finally {
      replicatingDocs.delete(doc.docId);
    }
  },

  /**
   * Periodic sweep to find documents with fewer than 3 deviceIds and attempt replication.
   */
  async replicationSweep() {
    const docs = await Document.find({
      $expr: { $lt: [{ $size: '$deviceIds' }, 3] }
    }).limit(1000);

    const concurrency = 10;
    for (let i = 0; i < docs.length; i += concurrency) {
      const batch = docs.slice(i, i + concurrency);
      await Promise.all(batch.map(doc => storageService.ensureReplication(doc)));
    }
  }
};

const webSocketManager = new WebSocketManager();
webSocketManager.startHeartbeat(30000);
webSocketManager.startReplicationSweep(60000);

module.exports = { webSocketManager, storageService };