# ORBITAL SURVIVOR multiplayer server starter

This is a starter room/signaling server, not a completed synchronized multiplayer game. It supports room creation/joining (up to 8 players), ready status, host start signal, and a host-to-room game state relay. The game HTML still needs Socket.IO client integration and authoritative synchronization for player movement, combat, enemies, bosses, and results.

## Run locally

```bash
npm install
npm start
```

The server listens on `PORT` (default 3000).
