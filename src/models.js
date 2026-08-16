const mongoose = require('mongoose');
const { v4: uuidv4 } = require('uuid');

const phoneSchema = new mongoose.Schema({
  deviceId: { type: String, unique: true, required: true, index: true },
  status: { type: String, enum: ['online', 'offline'], default: 'offline' },
  lastSeen: { type: Date, default: Date.now },
  createdAt: { type: Date, default: Date.now }
});

const documentSchema = new mongoose.Schema({
  docId: { type: String, unique: true, default: uuidv4, index: true },
  createdAt: { type: Date, default: Date.now },
  mainDeviceId: { type: String, required: true },
  backupDevicesIds: [{ type: String }],
  replicationStatus: { type: String, enum: ['pending', 'complete'], default: 'pending' } // NEW
});

const Phone = mongoose.model('Phone', phoneSchema);
const Document = mongoose.model('Document', documentSchema);

module.exports = { Phone, Document };