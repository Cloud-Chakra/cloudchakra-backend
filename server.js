require('dotenv').config();
const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const mongoose = require('mongoose');
const WebSocket = require('ws');
const config = require('./src/config');
const { Phone } = require('./src/models');
const { webSocketManager, storageService } = require('./src/services');
const router = require('./src/routes');

const app = express();

app.use(helmet());
app.use(cors());
app.use(express.json({ limit: '15mb' }));

app.use('/', router);

mongoose.connect(config.mongodbUri, {
  useNewUrlParser: true,
  useUnifiedTopology: true,
})
.then(() => console.log('MongoDB connected'))
.catch(err => {
  console.error('MongoDB connection error:', err);
  process.exit(1);
});

const server = app.listen(config.port, () => {
  console.log(`Server running on port ${config.port}`);
});

const wss = new WebSocket.Server({ server, path: '/ws' });

wss.on('connection', (ws, req) => {
  let authenticated = false;
  let deviceId = null;

  ws.on('message', async (data) => {
    try {
      const message = JSON.parse(data.toString());
      if (!authenticated && message.type === 'AUTHENTICATE') {
        const { token, deviceId: incomingDeviceId } = message;
        if (token !== config.phoneToken) {
          ws.send(JSON.stringify({ type: 'AUTH_FAILED', error: 'Invalid token' }));
          ws.close();
          return;
        }
        const phone = await Phone.findOne({ deviceId: incomingDeviceId });
        if (!phone) {
          ws.send(JSON.stringify({ type: 'AUTH_FAILED', error: 'Unknown deviceId' }));
          ws.close();
          return;
        }
        authenticated = true;
        deviceId = incomingDeviceId;
        webSocketManager.addConnection(deviceId, ws);
        ws.send(JSON.stringify({ type: 'AUTHENTICATED' }));
        return;
      }
      if (!authenticated) {
        ws.send(JSON.stringify({ type: 'ERROR', error: 'Not authenticated' }));
        return;
      }
      switch (message.type) {
        case 'HEARTBEAT':
          await Phone.findOneAndUpdate({ deviceId }, { lastSeen: new Date() });
          ws.send(JSON.stringify({ type: 'HEARTBEAT_ACK' }));
          break;
        case 'STORE_RESPONSE':
        case 'RETRIEVE_RESPONSE':
          webSocketManager.handleResponse(deviceId, message);
          break;
        default:
          console.log('Unknown message type:', message.type);
      }
    } catch (err) {
      console.error('WebSocket message error:', err);
    }
  });

  ws.on('close', () => {
    if (deviceId) {
      webSocketManager.removeConnection(deviceId);
      storageService.handlePhoneOffline(deviceId).catch(console.error);
    }
  });

  ws.on('pong', () => {});
});

process.on('SIGTERM', () => {
  server.close(() => {
    mongoose.connection.close();
    process.exit(0);
  });
});