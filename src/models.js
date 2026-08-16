const mongoose = require('mongoose');
const { v4: uuidv4 } = require('uuid');

const phoneSchema = new mongoose.Schema({
  deviceId: { type: String, unique: true, required: true, index: true },
  status: { type: String, enum: ['online', 'offline'], default: 'offline' },
  lastSeen: { type: Date, default: Date.now },
  ip: { type: String },
  createdAt: { type: Date, default: Date.now }
});

const documentSchema = new mongoose.Schema({
  docId: { type: String, unique: true, default: uuidv4, index: true },
  createdAt: { type: Date, default: Date.now },
  originalSize: { type: Number, required: true },
  shards: [{
    shardIndex: { type: Number, required: true },
    deviceId: { type: String, required: true },
    shardId: { type: String, required: true },
    storedAt: { type: Date, default: Date.now },
    size: { type: Number, required: true }
  }]
});

const Phone = mongoose.model('Phone', phoneSchema);
const Document = mongoose.model('Document', documentSchema);

module.exports = { Phone, Document };