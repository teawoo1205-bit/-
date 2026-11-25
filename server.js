const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const crypto = require('crypto');

const app = express();
app.get('/', (_req, res) => res.json({ name: 'ORBITAL SURVIVOR multiplayer server', status: 'online' }));
app.get('/health', (_req, res) => res.json({ ok: true }));
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });
const rooms = new Map();
const makeCode = () => String(crypto.randomInt(0, 1000000)).padStart(6, '0');

io.on('connection', socket => {
  socket.on('room:create', ({ mode } = {}, reply = () => {}) => {
    if (!['pvp', 'co-op'].includes(mode)) return reply({ ok: false, error: '잘못된 게임 모드입니다.' });
    let code; do { code = makeCode(); } while (rooms.has(code));
    const room = { code, mode, host: socket.id, players: new Map() };
    room.players.set(socket.id, { id: socket.id, ready: true });
    rooms.set(code, room); socket.join(code); socket.data.room = code;
    reply({ ok: true, code, mode, players: [...room.players.values()] });
    io.to(code).emit('room:update', publicRoom(room));
  });
  socket.on('room:join', ({ code } = {}, reply = () => {}) => {
    const room = rooms.get(String(code || ''));
    if (!room) return reply({ ok: false, error: '방을 찾을 수 없습니다.' });
    if (room.players.size >= 8) return reply({ ok: false, error: '방이 가득 찼습니다.' });
    room.players.set(socket.id, { id: socket.id, ready: false });
    socket.join(room.code); socket.data.room = room.code;
    reply({ ok: true, code: room.code, mode: room.mode, players: [...room.players.values()] });
    io.to(room.code).emit('room:update', publicRoom(room));
  });
  socket.on('room:ready', ({ ready } = {}) => {
    const room = rooms.get(socket.data.room); if (!room || !room.players.has(socket.id)) return;
    room.players.get(socket.id).ready = !!ready; io.to(room.code).emit('room:update', publicRoom(room));
  });
  socket.on('room:start', (_data, reply = () => {}) => {
    const room = rooms.get(socket.data.room);
    if (!room || room.host !== socket.id) return reply({ ok: false, error: '방장만 시작할 수 있습니다.' });
    io.to(room.code).emit('game:start', { mode: room.mode, players: [...room.players.values()] });
    reply({ ok: true });
  });
  socket.on('game:state', state => {
    const room = rooms.get(socket.data.room); if (!room || room.host !== socket.id) return;
    socket.to(room.code).emit('game:state', state);
  });
  socket.on('disconnect', () => {
    const code = socket.data.room, room = rooms.get(code); if (!room) return;
    room.players.delete(socket.id);
    if (!room.players.size) rooms.delete(code);
    else { if (room.host === socket.id) room.host = room.players.keys().next().value; io.to(code).emit('room:update', publicRoom(room)); }
  });
});
function publicRoom(room) { return { code: room.code, mode: room.mode, host: room.host, players: [...room.players.values()] }; }
const port = process.env.PORT || 3000;
server.listen(port, '0.0.0.0', () => console.log(`Listening on ${port}`));
