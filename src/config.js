require('dotenv').config();

const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017/cloudchakra';
const API_KEY = process.env.API_KEY || 'default-api-key';
const PHONE_TOKEN = process.env.PHONE_TOKEN || 'default-phone-token';
const ENCRYPTION_KEY = process.env.ENCRYPTION_KEY || Buffer.from('0123456789abcdef0123456789abcdef').toString('base64');

const config = {
  port: parseInt(process.env.PORT, 10) || 4000,
  mongodbUri: MONGODB_URI,
  apiKey: API_KEY,
  phoneToken: PHONE_TOKEN,
  encryptionKey: ENCRYPTION_KEY,
  heartbeatTimeout: 60 * 1000,
  shardCount: 3,
  dataShards: 2,
  parityShards: 1,
  maxDataSize: 15 * 1024 * 1024
};

module.exports = config;