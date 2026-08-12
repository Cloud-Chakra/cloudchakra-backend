const crypto = require('crypto');
const WebSocket = require('ws');
const { Phone, Document } = require('./models');
const config = require('./config');

// ---------- Encryption ----------
const encryptionKey = Buffer.from(config.encryptionKey, 'base64');
if (encryptionKey.length !== 32) {
  throw new Error('ENCRYPTION_KEY must be exactly 32 bytes when base64-decoded');
}

function encryptJSON(jsonObj) {
  const jsonStr = JSON.stringify(jsonObj);
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv('aes-256-gcm', encryptionKey, iv);
  let encrypted = cipher.update(jsonStr, 'utf8', 'base64');
  encrypted += cipher.final('base64');
  const authTag = cipher.getAuthTag().toString('base64');
  return {
    encrypted,
    iv: iv.toString('base64'),
    authTag,
    originalSize: Buffer.byteLength(jsonStr, 'utf8')
  };
}

function decryptJSON(encryptedBase64, ivBase64, authTagBase64) {
  const iv = Buffer.from(ivBase64, 'base64');
  const authTag = Buffer.from(authTagBase64, 'base64');
  const decipher = crypto.createDecipheriv('aes-256-gcm', encryptionKey, iv);
  decipher.setAuthTag(authTag);
  let decrypted = decipher.update(encryptedBase64, 'base64', 'utf8');
  decrypted += decipher.final('utf8');
  return JSON.parse(decrypted);
}

// ---------- Erasure Coding (XOR parity) ----------
function createShards(encryptedStr) {
  let buf = Buffer.from(encryptedStr, 'base64');
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
    paddingLength: padding,
    encryptedSize: buf.length
  };
}

function reconstructEncrypted(shardA, shardB, shardIndexes) {
  const halfLen = shardA.length;
  let dataShard1, dataShard2;
  if (shardIndexes.includes(0) && shardIndexes.includes(1)) {
    dataShard1 = shardA;
    dataShard2 = shardB;
  } else if (shardIndexes.includes(0) && shardIndexes.includes(2)) {
    dataShard1 = shardA;
    dataShard2 = Buffer.alloc(halfLen);
    for (let i = 0; i < halfLen; i++) dataShard2[i] = shardA[i] ^ shardB[i];
  } else if (shardIndexes.includes(1) && shardIndexes.includes(2)) {
    dataShard2 = shardA;
    dataShard1 = Buffer.alloc(halfLen);
    for (let i = 0; i < halfLen; i++) dataShard1[i] = shardA[i] ^ shardB[i];
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
    if (this.connections.has(deviceId)) {
      this.connections.get(deviceId).terminate();
    }
    this.connections.set(deviceId, ws);
    Phone.findOneAndUpdate(
      { deviceId },
      { status: 'online', lastSeen: new Date() },
      { upsert: true }
    ).catch(err => console.error('Error updating phone status:', err));
  }

  removeConnection(deviceId) {
    this.connections.delete(deviceId);
    Phone.findOneAndUpdate(
      { deviceId },
      { status: 'offline' }
    ).catch(err => console.error('Error updating phone status:', err));
  }

  isOnline(deviceId) {
    const ws = this.connections.get(deviceId);
    return ws && ws.readyState === WebSocket.OPEN;
  }

  getOnlinePhones() {
    const online = [];
    for (const [deviceId, ws] of this.connections.entries()) {
      if (ws.readyState === WebSocket.OPEN) online.push(deviceId);
    }
    return online;
  }

  sendRequest(deviceId, type, payload, timeout = 15000) {
    return new Promise((resolve, reject) => {
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
      this.pendingRequests.set(requestId, { resolve, reject, type });
      ws.send(JSON.stringify(message));
    });
  }

  handleResponse(deviceId, message) {
    const { requestId, success, error, data } = message;
    const pending = this.pendingRequests.get(requestId);
    if (!pending) return;
    this.pendingRequests.delete(requestId);
    if (success) pending.resolve(data || {});
    else pending.reject(new Error(error || 'Unknown error'));
  }

  startHeartbeat(interval = 60000) {
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
    const { encrypted, iv, authTag, originalSize } = encryptJSON(jsonObj);
    const { shards, paddingLength, encryptedSize } = createShards(encrypted);
    const onlinePhones = webSocketManager.getOnlinePhones();
    if (onlinePhones.length < 3) {
      throw new Error(`Need at least 3 online phones, currently ${onlinePhones.length}`);
    }
    const selectedPhones = [...onlinePhones].sort(() => Math.random() - 0.5).slice(0, 3);
    const shardResults = [];
    await Promise.all(shards.map(async (shard, index) => {
      const deviceId = selectedPhones[index];
      const shardId = crypto.randomBytes(8).toString('hex');  // generate shardId
      const payload = {
        shardIndex: index,
        data: shard.toString('base64'),
        docId: null,
        shardId
      };
      await webSocketManager.sendRequest(deviceId, 'STORE_SHARD', payload);
      shardResults.push({
        shardIndex: index,
        deviceId,
        shardId,   // store shardId in metadata
        size: shard.length,
        storedAt: new Date()
      });
    }));
    const doc = new Document({
      originalSize,
      encryptedSize,
      iv,
      authTag,
      paddingLength,
      shards: shardResults
    });
    await doc.save();
    return doc.docId;
  },

  async retrieveData(docId) {
    const doc = await Document.findOne({ docId });
    if (!doc) throw new Error('Document not found');
    const onlineShards = doc.shards.filter(s => webSocketManager.isOnline(s.deviceId));
    if (onlineShards.length < 2) throw new Error('Not enough shards online to reconstruct');
    const chosenShards = [];
    const dataShards = onlineShards.filter(s => s.shardIndex !== 2);
    if (dataShards.length >= 2) {
      chosenShards.push(dataShards[0], dataShards[1]);
    } else {
      chosenShards.push(onlineShards.find(s => s.shardIndex !== 2));
      chosenShards.push(onlineShards.find(s => s.shardIndex === 2));
    }
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
    const encryptedBuffer = reconstructEncrypted(shardBuffers[0], shardBuffers[1], indexes);
    const encryptedBase64 = encryptedBuffer.toString('base64');
    return decryptJSON(encryptedBase64, doc.iv, doc.authTag);
  },

  async handlePhoneOffline(deviceId) {
    const docs = await Document.find({ 'shards.deviceId': deviceId });
    for (const doc of docs) {
      const lostShard = doc.shards.find(s => s.deviceId === deviceId);
      if (!lostShard) continue;
      const otherShards = doc.shards.filter(s => s.deviceId !== deviceId);
      const onlineOther = otherShards.filter(s => webSocketManager.isOnline(s.deviceId));
      if (onlineOther.length < 2) continue;
      const onlinePhones = webSocketManager.getOnlinePhones();
      const candidates = onlinePhones.filter(d => d !== deviceId && !doc.shards.some(s => s.deviceId === d));
      if (candidates.length === 0) continue;
      const newPhone = candidates[0];
      const shardFetchPromises = onlineOther.map(shardInfo =>
        webSocketManager.sendRequest(shardInfo.deviceId, 'RETRIEVE_SHARD', { shardId: shardInfo.shardId })
      );
      const fetched = await Promise.all(shardFetchPromises);
      const otherShardData = fetched.map(f => Buffer.from(f.data, 'base64'));
      const otherIndexes = onlineOther.map(s => s.shardIndex);
      let missingShardBuffer;
      if (lostShard.shardIndex === 2) {
        const data0 = otherShardData[otherIndexes.indexOf(0)];
        const data1 = otherShardData[otherIndexes.indexOf(1)];
        missingShardBuffer = Buffer.alloc(data0.length);
        for (let i = 0; i < data0.length; i++) missingShardBuffer[i] = data0[i] ^ data1[i];
      } else if (lostShard.shardIndex === 0) {
        const parity = otherShardData[otherIndexes.indexOf(2)];
        const data1 = otherShardData[otherIndexes.indexOf(1)];
        missingShardBuffer = Buffer.alloc(parity.length);
        for (let i = 0; i < parity.length; i++) missingShardBuffer[i] = parity[i] ^ data1[i];
      } else {
        const parity = otherShardData[otherIndexes.indexOf(2)];
        const data0 = otherShardData[otherIndexes.indexOf(0)];
        missingShardBuffer = Buffer.alloc(parity.length);
        for (let i = 0; i < parity.length; i++) missingShardBuffer[i] = parity[i] ^ data0[i];
      }
      const newShardId = crypto.randomBytes(8).toString('hex');
      await webSocketManager.sendRequest(newPhone, 'STORE_SHARD', {
        shardIndex: lostShard.shardIndex,
        data: missingShardBuffer.toString('base64'),
        docId: doc.docId,
        shardId: newShardId
      });
      await Document.findOneAndUpdate(
        { docId: doc.docId, 'shards.shardIndex': lostShard.shardIndex },
        { $set: { 'shards.$.deviceId': newPhone, 'shards.$.shardId': newShardId, 'shards.$.storedAt': new Date() } }
      );
      console.log(`Re-replicated shard ${lostShard.shardIndex} to ${newPhone}`);
    }
  }
};

// Instantiate WebSocketManager and start heartbeat (after storageService defined)
const webSocketManager = new WebSocketManager();
webSocketManager.startHeartbeat(30000);

module.exports = { webSocketManager, storageService, encryptJSON, decryptJSON };