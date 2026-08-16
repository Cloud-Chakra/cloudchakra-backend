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

    storageService.handlePhoneOffline(deviceId).catch(err =>
      console.error(`Error handling phone offline for ${deviceId}:`, err)
    );
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

      const allPhones = await Phone.find({ status: 'online' });
      for (const phone of allPhones) {
        if (!this.isOnline(phone.deviceId)) {
          phone.status = 'offline';
          await phone.save();
          await storageService.handlePhoneOffline(phone.deviceId);
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

// ---------- Storage Service (Replication-based) ----------
const storageService = {
  async storeData(jsonObj) {
    const jsonStr = JSON.stringify(jsonObj);
    const docId = uuidv4();

    const onlinePhones = webSocketManager.getOnlinePhones();
    if (onlinePhones.length === 0) {
      throw new Error('No online phones available to store data');
    }

    const mainDeviceId = onlinePhones[0];

    // Store on main device synchronously
    await webSocketManager.sendRequest(mainDeviceId, 'STORE_DOC', {
      docId,
      data: jsonStr
    });

    const doc = new Document({
      docId,
      mainDeviceId,
      backupDevicesIds: [],
      replicationStatus: 'pending'
    });
    await doc.save();

    setImmediate(() => {
      storageService.ensureReplication(doc).catch(err => {
        console.error(`Initial replication for doc ${docId} failed:`, err);
      });
    });

    return docId;
  },

  async retrieveData(docId) {
    const doc = await Document.findOne({ docId });
    if (!doc) throw new Error('Document not found');

    if (webSocketManager.isOnline(doc.mainDeviceId)) {
      try {
        const result = await webSocketManager.sendRequest(doc.mainDeviceId, 'RETRIEVE_DOC', { docId });
        return JSON.parse(result.data);
      } catch (err) {
        console.warn(`Failed to retrieve from main device ${doc.mainDeviceId}: ${err.message}`);
      }
    }

    for (const deviceId of doc.backupDevicesIds) {
      if (webSocketManager.isOnline(deviceId)) {
        try {
          const result = await webSocketManager.sendRequest(deviceId, 'RETRIEVE_DOC', { docId });
          return JSON.parse(result.data);
        } catch (err) {
          console.warn(`Failed to retrieve from backup device ${deviceId}: ${err.message}`);
        }
      }
    }

    throw new Error('No online copy available for this document');
  },

  async handlePhoneOffline(deviceId) {
    const docs = await Document.find({
      $or: [
        { mainDeviceId: deviceId },
        { backupDevicesIds: { $in: [deviceId] } }
      ]
    });

    const concurrency = 5;
    for (let i = 0; i < docs.length; i += concurrency) {
      const batch = docs.slice(i, i + concurrency);
      await Promise.all(batch.map(doc => storageService.ensureReplication(doc)));
    }
  },

  async handlePhoneOnline(deviceId) {
    const docs = await Document.find({
      $expr: {
        $lt: [{ $size: { $ifNull: ['$backupDevicesIds', []] } }, 2]
      },
      mainDeviceId: { $ne: deviceId },
      backupDevicesIds: { $nin: [deviceId] }
    });

    const concurrency = 5;
    for (let i = 0; i < docs.length; i += concurrency) {
      const batch = docs.slice(i, i + concurrency);
      await Promise.all(batch.map(doc => storageService.ensureReplication(doc)));
    }
  },

  /**
   * Ensure a document has at least 3 online copies.
   * - Promotes a backup to main if main is offline.
   * - Prunes offline backups from backupDevicesIds.
   * - Replicates to new devices if needed.
   */
  async ensureReplication(doc) {
    // Prevent concurrent replication for the same doc
    if (replicatingDocs.has(doc.docId)) {
      console.log(`Replication already in progress for doc ${doc.docId}, skipping.`);
      return;
    }
    replicatingDocs.add(doc.docId);

    try {
      // 1. Ensure backupDevicesIds is an array
      if (!Array.isArray(doc.backupDevicesIds)) {
        doc.backupDevicesIds = [];
      }

      // 2. Prune offline backups
      const onlineBackups = doc.backupDevicesIds.filter(id => webSocketManager.isOnline(id));
      if (onlineBackups.length !== doc.backupDevicesIds.length) {
        doc.backupDevicesIds = onlineBackups;
      }

      // 3. If main is offline, promote the first online backup (if any) to main
      if (!webSocketManager.isOnline(doc.mainDeviceId)) {
        if (onlineBackups.length > 0) {
          const newMain = onlineBackups[0];
          doc.backupDevicesIds = doc.backupDevicesIds.filter(id => id !== newMain);
          doc.mainDeviceId = newMain;
          console.log(`Promoted backup ${newMain} to main for doc ${doc.docId}`);
        } else {
          // Main is offline and no backups online, keep main as is (still offline)
          // but ensure it's not undefined
          if (!doc.mainDeviceId) {
            console.error(`Doc ${doc.docId} has no mainDeviceId and no online backups. Cannot promote.`);
            return; // Do not save
          }
        }
      }

      // 4. Determine current online copies after promotion/pruning
      const allCopyDevices = [doc.mainDeviceId, ...doc.backupDevicesIds];
      const onlineCopyDevices = allCopyDevices.filter(id => webSocketManager.isOnline(id));

      // 5. If already have 3 online copies, mark complete and return
      if (onlineCopyDevices.length >= 3) {
        if (doc.replicationStatus !== 'complete') {
          doc.replicationStatus = 'complete';
          await doc.save();
        }
        return;
      }

      // 6. Calculate how many additional copies we need
      const needed = 3 - onlineCopyDevices.length;
      const onlinePhones = webSocketManager.getOnlinePhones();
      const candidates = onlinePhones.filter(id => !allCopyDevices.includes(id));

      // If no candidates, we cannot complete replication now
      if (candidates.length === 0) {
        console.log(`No candidates to replicate doc ${doc.docId}. Remaining pending.`);
        await doc.save(); // persist any pruning/promotion changes
        return;
      }

      // 7. Need source data from an existing online copy
      if (onlineCopyDevices.length === 0) {
        console.warn(`Cannot replicate doc ${doc.docId}: no online copy available to source data`);
        await doc.save();
        return;
      }

      const sourceDeviceId = onlineCopyDevices[0];
      let dataStr;
      try {
        const result = await webSocketManager.sendRequest(sourceDeviceId, 'RETRIEVE_DOC', { docId: doc.docId });
        dataStr = result.data;
        if (typeof dataStr !== 'string' || dataStr.length === 0) {
          throw new Error(`Invalid data received from source device ${sourceDeviceId}`);
        }
      } catch (err) {
        console.error(`Failed to retrieve valid data from source ${sourceDeviceId} for replication: ${err.message}`);
        await doc.save();
        return;
      }

      // 8. Replicate to up to `needed` new devices
      const devicesToAdd = candidates.slice(0, needed);
      for (const newDeviceId of devicesToAdd) {
        try {
          await webSocketManager.sendRequest(newDeviceId, 'STORE_DOC', {
            docId: doc.docId,
            data: dataStr
          });
          doc.backupDevicesIds.push(newDeviceId);
          console.log(`Replicated doc ${doc.docId} to ${newDeviceId}`);
        } catch (err) {
          console.error(`Failed to replicate to ${newDeviceId}: ${err.message}`);
        }
      }

      // 9. Re-evaluate online copies and update status
      const updatedOnlineCopies = [doc.mainDeviceId, ...doc.backupDevicesIds]
        .filter(id => webSocketManager.isOnline(id)).length;
      doc.replicationStatus = updatedOnlineCopies >= 3 ? 'complete' : 'pending';
      await doc.save();
    } finally {
      replicatingDocs.delete(doc.docId);
    }
  },

  /**
   * Periodic sweep to heal under-replicated documents.
   * Only processes documents that are marked pending or have fewer than 2 backups.
   */
  async replicationSweep() {
    const docs = await Document.find({
      $or: [
        { replicationStatus: 'pending' },
        { $expr: { $lt: [{ $size: { $ifNull: ['$backupDevicesIds', []] } }, 2] } }
      ]
    }).limit(1000);

    const concurrency = 10;
    for (let i = 0; i < docs.length; i += concurrency) {
      const batch = docs.slice(i, i + concurrency);
      await Promise.all(batch.map(doc => storageService.ensureReplication(doc)));
    }
  }
};

// Instantiate WebSocketManager and start background tasks
const webSocketManager = new WebSocketManager();
webSocketManager.startHeartbeat(30000);
webSocketManager.startReplicationSweep(60000);

module.exports = { webSocketManager, storageService };