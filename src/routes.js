const express = require('express');
const { Phone, Document } = require('./models');
const { storageService } = require('./services');
const config = require('./config');

const router = express.Router();

const requireApiKey = (req, res, next) => {
  const apiKey = req.headers['x-api-key'];
  if (!apiKey || apiKey !== config.apiKey) {
    return res.status(401).json({ error: 'Invalid or missing API key' });
  }
  next();
};

router.post('/add/phone', requireApiKey, async (req, res) => {
  try {
    const { deviceId } = req.body;
    if (!deviceId) return res.status(400).json({ error: 'deviceId is required' });
    const phone = await Phone.findOneAndUpdate(
      { deviceId },
      { deviceId, status: 'offline' },
      { upsert: true, new: true }
    );
    res.json({ success: true, phone });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.post('/api/store', requireApiKey, async (req, res) => {
  try {
    const jsonData = req.body;
    if (!jsonData || typeof jsonData !== 'object') {
      return res.status(400).json({ error: 'Request body must be a JSON object' });
    }
    const size = Buffer.byteLength(JSON.stringify(jsonData));
    if (size > config.maxDataSize) {
      return res.status(413).json({ error: 'Payload too large' });
    }
    const docId = await storageService.storeData(jsonData);
    res.status(201).json({ success: true, docId });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.get('/api/retrieve/:docId', requireApiKey, async (req, res) => {
  try {
    const { docId } = req.params;
    const data = await storageService.retrieveData(docId);
    res.json({ success: true, data });
  } catch (err) {
    res.status(404).json({ error: err.message });
  }
});

router.get('/api/docs', requireApiKey, async (req, res) => {
  try {
    const docs = await Document.find({}, 'docId createdAt originalSize encryptedSize shards');
    res.json({ success: true, documents: docs });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;