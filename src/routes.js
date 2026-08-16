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

// Register a phone
router.post('/add/phone', requireApiKey, async (req, res) => {
  try {
    const { deviceId } = req.body;
    if (!deviceId || typeof deviceId !== 'string' || deviceId.trim() === '') {
      return res.status(400).json({ error: 'deviceId is required and must be a non-empty string' });
    }

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

// Store a single document
router.post('/api/store', requireApiKey, async (req, res) => {
  const start = Date.now();

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
    const latencyMs = Date.now() - start;

    res.status(201).json({ success: true, docId, latencyMs });
  } catch (err) {
    res.status(500).json({ error: err.message, latencyMs: Date.now() - start });
  }
});

// Retrieve a single document
router.get('/api/retrieve/:docId', requireApiKey, async (req, res) => {
  const start = Date.now();

  try {
    const { docId } = req.params;
    const data = await storageService.retrieveData(docId);
    const latencyMs = Date.now() - start;

    res.json({ success: true, data, latencyMs });
  } catch (err) {
    res.status(404).json({ error: err.message, latencyMs: Date.now() - start });
  }
});

// Bulk store: expects { "documents": [ { ... }, { ... }, ... ] }
router.post('/api/bulk-store', requireApiKey, async (req, res) => {
  const start = Date.now();

  try {
    const { documents } = req.body;
    if (!Array.isArray(documents) || documents.length === 0) {
      return res.status(400).json({ error: 'documents must be a non-empty array' });
    }

    const docIds = [];
    const errors = [];

    for (let i = 0; i < documents.length; i++) {
      const jsonData = documents[i];
      if (!jsonData || typeof jsonData !== 'object') {
        errors.push({ index: i, error: 'Item must be an object' });
        continue;
      }
      try {
        const docId = await storageService.storeData(jsonData);
        docIds.push({ index: i, docId });
      } catch (err) {
        errors.push({ index: i, error: err.message });
      }
    }

    const latencyMs = Date.now() - start;
    res.status(201).json({ success: true, docIds, errors, latencyMs });
  } catch (err) {
    res.status(500).json({ error: err.message, latencyMs: Date.now() - start });
  }
});

// Get all docs (metadata only by default, or with data if includeData=true)
router.get('/api/docs', requireApiKey, async (req, res) => {
  try {
    const includeData = req.query.includeData === 'true';
    const docs = await Document.find({}, 'docId createdAt originalSize shards');

    if (includeData) {
      const docsWithData = [];
      for (const doc of docs) {
        try {
          const data = await storageService.retrieveData(doc.docId);
          docsWithData.push({ ...doc.toObject(), data });
        } catch (err) {
          docsWithData.push({ ...doc.toObject(), data: null, error: err.message });
        }
      }
      return res.json({ success: true, documents: docsWithData });
    }

    res.json({ success: true, documents: docs });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;