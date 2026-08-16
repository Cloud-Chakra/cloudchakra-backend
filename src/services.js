const crypto = require('crypto');
const WebSocket = require('ws');
const { v4: uuidv4 } = require('uuid');
const { Phone, Document } = require('./models');
const config = require('./config');

// ---------- XOR Erasure Coding (no encryption) ----------
function createShards(jsonStr) {
  let buf = Buffer.from(jsonStr, 'utf8');
  let padding = 0;
  if (buf.length % 2 !== 0) {
    padding = 1;
    buf = Buffer.concat([buf, Buffer.alloc(1)]);
  }

  const half = buf.length / 2;
  const shard1 = buf.subarray(0, half);
  const shard2 = buf.subarray(half);
  const parity = Buffer.alloc(half);

  for (let i = 0; i < half; i++) {
    parity[i] = shard1[i] ^ shard2[i];
  }

  return {
    shards: [shard1, shard2, parity],
    paddingLength: padding
  };
}

function reconstructBuffer(shardA, shardB, indexA, indexB) {
  const halfLen = shardA.length;
  const shards = { [indexA]: shardA, [indexB]: shardB };
  const sortedIndexes = [indexA, indexB].sort((a, b) => a - b);

  let dataShard1, dataShard2;

  if (sortedIndexes[0] === 0 && sortedIndexes[1] === 1) {
    dataShard1 = shards[0];
    dataShard2 = shards[1];
  } else if (sortedIndexes[0] === 0 && sortedIndexes[1] === 2) {
    dataShard1 = shards[0];
    dataShard2 = Buffer.alloc(halfLen);
    for (let i = 0; i < halfLen; i++) {
      dataShard2[i] = shards[0][i] ^ shards[2][i];
    }
  } else if (sortedIndexes[0] === 1 && sortedIndexes[1] === 2) {
    dataShard1 = Buffer.alloc(halfLen);
    dataShard2 = shards[1];
    for (let i = 0; i < halfLen; i++) {
      dataShard1[i] = shards[1][i] ^ shards[2][i];
    }
  } else {
    throw new Error('Invalid shard combination');
  }

  return Buffer.concat([dataShard1, dataShard2]);
}

// ---------- WebSocket Manager ----------
class WebSocketManager {
  constructor() {
    this.connections = new Map();
    this.pendingRequests = new Map();
    this.heartbeatInterval = null;
  }

  addConnection(deviceId, ws) {
    if (!deviceId || typeof deviceId !== 'string' || deviceId.trim() === '') {
      console.error('[WebSocketManager] Rejecting connection with invalid deviceId:', deviceId);
      return;
    }

    if (this.connections.has(deviceId)) {
      const current = this.connections.get(deviceId);
      if (current === ws) return; // already tracked
      console.log('[WebSocketManager] Replacing existing connection for', deviceId);
      current.terminate(); // will trigger close on old ws, but we ignore it later
    }

    this.connections.set(deviceId, ws);
    console.log('[WebSocketManager] Connection added. Online count:', this.getOnlinePhones().length);

    Phone.findOneAndUpdate(
      { deviceId },
      { status: 'online', lastSeen: new Date() },
      { upsert: true }
    ).catch(err => console.error('Error updating phone status:', err));
  }

  removeConnection(deviceId, ws) {
    if (!deviceId || typeof deviceId !== 'string') return;

    const current = this.connections.get(deviceId);
    if (current && current !== ws) {
      // A newer connection has replaced this one; do not remove
      console.log(`[WebSocketManager] Ignoring close of old connection for ${deviceId}`);
      return;
    }

    this.connections.delete(deviceId);
    console.log('[WebSocketManager] Connection removed. Online count:', this.getOnlinePhones().length);

    Phone.findOneAndUpdate(
      { deviceId },
      { status: 'offline' }
    ).catch(err => console.error('Error updating phone status:', err));
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
    console.log('[WebSocketManager] Online phones list:', online);
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
}

// ---------- Storage Service ----------
const storageService = {
  async storeData(jsonObj) {
    const jsonStr = JSON.stringify(jsonObj);
    const { shards } = createShards(jsonStr);
    const docId = uuidv4(); // generate upfront, used in all shards

    let onlinePhones = webSocketManager.getOnlinePhones().filter(id => typeof id === 'string' && id.trim() !== '');
    if (onlinePhones.length === 0) {
      throw new Error('No online phones available to store data');
    }

    // Build assignment plan: array of { shardIndex, deviceId }
    const assignments = [];

    if (onlinePhones.length === 1) {
      const phone = onlinePhones[0];
      for (let i = 0; i < 3; i++) {
        assignments.push({ shardIndex: i, deviceId: phone });
      }
    } else if (onlinePhones.length === 2) {
      // Two phones: each phone gets two shards for redundancy
      const phone0 = onlinePhones[0];
      const phone1 = onlinePhones[1];
      // phone0: shard0 & shard1, phone1: shard0 & shard2
      assignments.push({ shardIndex: 0, deviceId: phone0 });
      assignments.push({ shardIndex: 1, deviceId: phone0 });
      assignments.push({ shardIndex: 0, deviceId: phone1 });
      assignments.push({ shardIndex: 2, deviceId: phone1 });
    } else {
      // Three or more phones: use distinct phones for each shard
      const selected = onlinePhones.slice(0, 3);
      for (let i = 0; i < 3; i++) {
        assignments.push({ shardIndex: i, deviceId: selected[i] });
      }
    }

    const shardResults = [];

    await Promise.all(assignments.map(async (assignment) => {
      const { shardIndex, deviceId } = assignment;
      const shard = shards[shardIndex];
      const shardId = crypto.randomBytes(8).toString('hex');
      const payload = {
        shardIndex,
        data: shard.toString('base64'),
        docId,
        shardId
      };

      await webSocketManager.sendRequest(deviceId, 'STORE_SHARD', payload);

      shardResults.push({
        shardIndex,
        deviceId,
        shardId,
        size: shard.length,
        storedAt: new Date()
      });
    }));

    const doc = new Document({
      docId,
      originalSize: Buffer.byteLength(jsonStr, 'utf8'),
      shards: shardResults
    });
    await doc.save();
    return doc.docId;
  },

  async retrieveData(docId) {
    const doc = await Document.findOne({ docId });
    if (!doc) throw new Error('Document not found');

    const onlineShards = doc.shards.filter(s => webSocketManager.isOnline(s.deviceId));
    if (onlineShards.length === 0) {
      throw new Error('No shards online for this document');
    }

    // Get unique shard indexes that are online
    const availableIndexes = [...new Set(onlineShards.map(s => s.shardIndex))];
    if (availableIndexes.length < 2) {
      throw new Error('Not enough distinct shards online to reconstruct');
    }

    // Choose any two distinct indexes (first two in list)
    const chosenIndexes = [availableIndexes[0], availableIndexes[1]];
    const chosenShards = chosenIndexes.map(idx => onlineShards.find(s => s.shardIndex === idx));

    const shardDataPromises = chosenShards.map(async (shardInfo) => {
      const result = await webSocketManager.sendRequest(
        shardInfo.deviceId,
        'RETRIEVE_SHARD',
        { shardId: shardInfo.shardId }
      );
      return { index: shardInfo.shardIndex, data: result.data };
    });

    const retrieved = await Promise.all(shardDataPromises);
    const shardBuffers = retrieved.map(r => Buffer.from(r.data, 'base64'));
    const indexes = retrieved.map(r => r.index);
    const reconstructedBuffer = reconstructBuffer(shardBuffers[0], shardBuffers[1], indexes[0], indexes[1]);

    const jsonStr = reconstructedBuffer.toString('utf8').replace(/\0+$/, '');
    return JSON.parse(jsonStr);
  },

  async handlePhoneOffline(deviceId) {
    if (!deviceId || typeof deviceId !== 'string') return;

    const docs = await Document.find({ 'shards.deviceId': deviceId });
    if (docs.length === 0) return;

    const onlinePhones = webSocketManager.getOnlinePhones()
      .filter(id => id !== deviceId && typeof id === 'string' && id.trim() !== '');

    for (const doc of docs) {
      // Determine which shard indexes are now missing online copies
      const missingIndexes = [];
      for (let idx = 0; idx < 3; idx++) {
        const hasOnlineCopy = doc.shards.some(
          s => s.shardIndex === idx && s.deviceId !== deviceId && webSocketManager.isOnline(s.deviceId)
        );
        if (!hasOnlineCopy) {
          missingIndexes.push(idx);
        }
      }

      if (missingIndexes.length === 0) continue;

      for (const missingIdx of missingIndexes) {
        // We need the other two shard indexes to reconstruct
        const otherIndexes = [0, 1, 2].filter(i => i !== missingIdx);
        const copiesForOther = otherIndexes.map(idx =>
          doc.shards.find(s =>
            s.shardIndex === idx &&
            s.deviceId !== deviceId &&
            webSocketManager.isOnline(s.deviceId)
          )
        );

        if (copiesForOther.some(c => !c)) {
          console.warn(`Cannot reconstruct shard ${missingIdx} for doc ${doc.docId}: missing other shards online`);
          continue;
        }

        // Fetch the two other shards
        const fetched = await Promise.all(copiesForOther.map(async (shardInfo) => {
          const result = await webSocketManager.sendRequest(
            shardInfo.deviceId,
            'RETRIEVE_SHARD',
            { shardId: shardInfo.shardId }
          );
          return { index: shardInfo.shardIndex, data: Buffer.from(result.data, 'base64') };
        }));

        const [shardA, shardB] = fetched;
        const indexA = shardA.index;
        const indexB = shardB.index;

        let missingBuffer;
        if (missingIdx === 0) {
          // shard0 = shard1 XOR shard2
          if ((indexA === 1 && indexB === 2) || (indexA === 2 && indexB === 1)) {
            missingBuffer = Buffer.alloc(shardA.data.length);
            for (let i = 0; i < shardA.data.length; i++) {
              missingBuffer[i] = shardA.data[i] ^ shardB.data[i];
            }
          } else {
            console.error('Wrong shards to reconstruct shard0');
            continue;
          }
        } else if (missingIdx === 1) {
          // shard1 = shard0 XOR shard2
          if ((indexA === 0 && indexB === 2) || (indexA === 2 && indexB === 0)) {
            missingBuffer = Buffer.alloc(shardA.data.length);
            for (let i = 0; i < shardA.data.length; i++) {
              missingBuffer[i] = shardA.data[i] ^ shardB.data[i];
            }
          } else {
            console.error('Wrong shards to reconstruct shard1');
            continue;
          }
        } else { // missingIdx === 2
          // parity = shard0 XOR shard1
          if ((indexA === 0 && indexB === 1) || (indexA === 1 && indexB === 0)) {
            missingBuffer = Buffer.alloc(shardA.data.length);
            for (let i = 0; i < shardA.data.length; i++) {
              missingBuffer[i] = shardA.data[i] ^ shardB.data[i];
            }
          } else {
            console.error('Wrong shards to reconstruct shard2');
            continue;
          }
        }

        // Choose a new phone for the reconstructed shard
        const existingDevicesForIndex = doc.shards
          .filter(s => s.shardIndex === missingIdx)
          .map(s => s.deviceId);
        const candidates = onlinePhones.filter(p => !existingDevicesForIndex.includes(p));
        const newPhone = candidates.length > 0 ? candidates[0] : onlinePhones[0];

        if (!newPhone) {
          console.warn('No online phone to store reconstructed shard');
          continue;
        }

        const newShardId = crypto.randomBytes(8).toString('hex');
        await webSocketManager.sendRequest(newPhone, 'STORE_SHARD', {
          shardIndex: missingIdx,
          data: missingBuffer.toString('base64'),
          docId: doc.docId,
          shardId: newShardId
        });

        // Add new entry to document
        await Document.updateOne(
          { docId: doc.docId },
          {
            $push: {
              shards: {
                shardIndex: missingIdx,
                deviceId: newPhone,
                shardId: newShardId,
                size: missingBuffer.length,
                storedAt: new Date()
              }
            }
          }
        );

        console.log(`Re-replicated shard ${missingIdx} of doc ${doc.docId} to ${newPhone}`);
      }
    }
  }
};

// Instantiate WebSocketManager and start heartbeat
const webSocketManager = new WebSocketManager();
webSocketManager.startHeartbeat(30000);

module.exports = { webSocketManager, storageService };