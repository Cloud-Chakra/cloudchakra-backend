require('dotenv').config();

const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017/cloudchakra';
const API_KEY = process.env.API_KEY || 'default-api-key';
const PHONE_TOKEN = process.env.PHONE_TOKEN || 'default-phone-token';

const config = {
  port: parseInt(process.env.PORT, 10) || 4000,
  mongodbUri: MONGODB_URI,
  apiKey: API_KEY,
  phoneToken: PHONE_TOKEN,
  heartbeatTimeout: 60 * 1000,
  maxDataSize: 15 * 1024 * 1024, // 15 MB per document
  requestTimeout: 15000 // WebSocket request timeout in ms (15s)
};

module.exports = config;