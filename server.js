// ============================================================================
// corcod: WebSocket Signaling Server
// Real-time WebRTC coordination, chat relay, and presence management
// ============================================================================

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);
const io = new Server(server, {
    cors: {
        origin: '*',
        methods: ['GET', 'POST']
    },
    pingInterval: 10000,
    pingTimeout: 5000,
    transports: ['websocket', 'polling']
});

// ============================================================================
// IN-MEMORY STATE (replaces MySQL for transient data)
// ============================================================================

// rooms: Map<roomId, Map<userId, { socketId, username, avatar, role, joinedAt }>>
const rooms = new Map();

// socketToUser: Map<socketId, { userId, roomId, username }>
const socketToUser = new Map();

// chatHistory: Map<roomId, Array<{ userId, username, avatar, message, time }>>
const chatHistory = new Map();

const MAX_CHAT_HISTORY = 100;

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

function getRoomMembers(roomId) {
    const room = rooms.get(roomId);
    if (!room) return [];
    return Array.from(room.entries()).map(([userId, data]) => ({
        user_id: userId,
        username: data.username,
        avatar: data.avatar || '',
        role: data.role,
        socketId: data.socketId
    }));
}

function removeUserFromRoom(socketId) {
    const userData = socketToUser.get(socketId);
    if (!userData) return null;

    const { userId, roomId, username } = userData;
    const room = rooms.get(roomId);

    if (room) {
        room.delete(userId);
        if (room.size === 0) {
            rooms.delete(roomId);
            chatHistory.delete(roomId);
        }
    }

    socketToUser.delete(socketId);
    return { userId, roomId, username };
}

function timestamp() {
    const d = new Date();
    return d.getHours().toString().padStart(2, '0') + ':' +
           d.getMinutes().toString().padStart(2, '0');
}

// ============================================================================
// SOCKET EVENT HANDLERS
// ============================================================================

io.on('connection', (socket) => {
    console.log(`[CONNECT] Socket ${socket.id} connected`);

    // ------------------------------------------------------------------
    // JOIN ROOM
    // ------------------------------------------------------------------
    socket.on('join-room', (data) => {
        const { roomId, userId, username, avatar, role } = data;
        if (!roomId || !userId) return;

        const roomIdStr = String(roomId);
        const userIdNum = parseInt(userId);

        // Initialize room if new
        if (!rooms.has(roomIdStr)) {
            rooms.set(roomIdStr, new Map());
        }

        const room = rooms.get(roomIdStr);

        // Store user in room
        room.set(userIdNum, {
            socketId: socket.id,
            username: username || `User ${userIdNum}`,
            avatar: avatar || '',
            role: role || 'attendee',
            joinedAt: Date.now()
        });

        // Map socket to user
        socketToUser.set(socket.id, {
            userId: userIdNum,
            roomId: roomIdStr,
            username: username || `User ${userIdNum}`
        });

        // Join Socket.io room channel
        socket.join(`room_${roomIdStr}`);

        // Notify all peers in room about updated member list
        const members = getRoomMembers(roomIdStr);
        io.to(`room_${roomIdStr}`).emit('room-members', { members });

        // Send existing chat history to joining user
        const history = chatHistory.get(roomIdStr) || [];
        socket.emit('chat-history', { chats: history });

        console.log(`[JOIN] User ${userIdNum} (${username}) joined room ${roomIdStr}. Total: ${room.size}`);
    });

    // ------------------------------------------------------------------
    // WEBRTC SIGNALING RELAY
    // ------------------------------------------------------------------
    socket.on('signal', (data) => {
        const { targetUserId, type, payload } = data;
        const sender = socketToUser.get(socket.id);
        if (!sender) return;

        const roomIdStr = sender.roomId;
        const room = rooms.get(roomIdStr);
        if (!room) return;

        const targetUser = room.get(parseInt(targetUserId));
        if (!targetUser) return;

        // Relay signal directly to target peer's socket
        io.to(targetUser.socketId).emit('signal', {
            senderId: sender.userId,
            senderName: sender.username,
            type: type,
            payload: payload
        });
    });

    // ------------------------------------------------------------------
    // CHAT MESSAGE RELAY
    // ------------------------------------------------------------------
    socket.on('chat-message', (data) => {
        const sender = socketToUser.get(socket.id);
        if (!sender || !data.message) return;

        const roomIdStr = sender.roomId;
        const room = rooms.get(roomIdStr);
        if (!room) return;

        const senderData = room.get(sender.userId);
        const chatMsg = {
            user_id: sender.userId,
            username: sender.username,
            avatar: senderData ? senderData.avatar : '',
            message: data.message.substring(0, 2000), // Limit message length
            time: timestamp()
        };

        // Store in history (capped buffer)
        if (!chatHistory.has(roomIdStr)) {
            chatHistory.set(roomIdStr, []);
        }
        const history = chatHistory.get(roomIdStr);
        history.push(chatMsg);
        if (history.length > MAX_CHAT_HISTORY) {
            history.shift();
        }

        // Broadcast to entire room
        io.to(`room_${roomIdStr}`).emit('chat-message', chatMsg);
    });

    // ------------------------------------------------------------------
    // ACTIVE SPEAKER STATE
    // ------------------------------------------------------------------
    socket.on('speaking-state', (data) => {
        const sender = socketToUser.get(socket.id);
        if (!sender) return;

        socket.to(`room_${sender.roomId}`).emit('speaking-state', {
            userId: sender.userId,
            isSpeaking: !!data.isSpeaking
        });
    });

    // ------------------------------------------------------------------
    // WHITEBOARD RELAY (supplements DataChannel for late joiners)
    // ------------------------------------------------------------------
    socket.on('whiteboard-draw', (data) => {
        const sender = socketToUser.get(socket.id);
        if (!sender) return;

        socket.to(`room_${sender.roomId}`).emit('whiteboard-draw', {
            x1: data.x1, y1: data.y1,
            x2: data.x2, y2: data.y2,
            color: data.color, size: data.size
        });
    });

    socket.on('whiteboard-clear', () => {
        const sender = socketToUser.get(socket.id);
        if (!sender) return;

        socket.to(`room_${sender.roomId}`).emit('whiteboard-clear');
    });

    // ------------------------------------------------------------------
    // CUSTOM SIGNALS (hand raise, reactions, wishes, mute states)
    // ------------------------------------------------------------------
    socket.on('custom-signal', (data) => {
        const sender = socketToUser.get(socket.id);
        if (!sender) return;

        // Broadcast to all others in the room
        socket.to(`room_${sender.roomId}`).emit('custom-signal', {
            senderId: sender.userId,
            senderName: sender.username,
            payload: data.payload
        });
    });

    // ------------------------------------------------------------------
    // HOST CONTROLS (force mute, kick, lock)
    // ------------------------------------------------------------------
    socket.on('host-control', (data) => {
        const sender = socketToUser.get(socket.id);
        if (!sender) return;

        const { targetUserId, action } = data;
        const room = rooms.get(sender.roomId);
        if (!room) return;

        // Verify sender is host
        const senderData = room.get(sender.userId);
        if (!senderData || senderData.role !== 'host') return;

        const targetUser = room.get(parseInt(targetUserId));
        if (!targetUser) return;

        io.to(targetUser.socketId).emit('host-control', {
            action: action // 'force_mute', 'kick', 'lock_audio', 'unlock_audio', etc.
        });
    });

    // ------------------------------------------------------------------
    // ROOM TERMINATED (admin closes room)
    // ------------------------------------------------------------------
    socket.on('room-closed', (data) => {
        const sender = socketToUser.get(socket.id);
        if (!sender) return;

        io.to(`room_${sender.roomId}`).emit('room-closed', {
            message: 'This meeting has been terminated.'
        });

        // Clean up room state
        const room = rooms.get(sender.roomId);
        if (room) {
            room.forEach((userData, uid) => {
                socketToUser.delete(userData.socketId);
            });
            rooms.delete(sender.roomId);
            chatHistory.delete(sender.roomId);
        }
    });

    // ------------------------------------------------------------------
    // DISCONNECT
    // ------------------------------------------------------------------
    socket.on('disconnect', (reason) => {
        const removed = removeUserFromRoom(socket.id);
        if (removed) {
            const { userId, roomId, username } = removed;

            // Notify remaining room members
            const members = getRoomMembers(roomId);
            io.to(`room_${roomId}`).emit('room-members', { members });
            io.to(`room_${roomId}`).emit('peer-disconnected', { userId });

            console.log(`[DISCONNECT] User ${userId} (${username}) left room ${roomId}. Reason: ${reason}`);
        }
    });
});

// ============================================================================
// REST API (Health + Stats)
// ============================================================================

app.get('/api/health', (req, res) => {
    const totalRooms = rooms.size;
    let totalUsers = 0;
    rooms.forEach(room => { totalUsers += room.size; });

    res.json({
        status: 'ok',
        uptime: process.uptime(),
        rooms: totalRooms,
        users: totalUsers,
        connections: io.engine.clientsCount
    });
});

app.get('/api/rooms', (req, res) => {
    const roomList = [];
    rooms.forEach((members, roomId) => {
        roomList.push({
            roomId,
            participants: members.size,
            members: Array.from(members.entries()).map(([uid, d]) => ({
                userId: uid,
                username: d.username,
                role: d.role
            }))
        });
    });
    res.json({ rooms: roomList });
});

// ============================================================================
// START SERVER
// ============================================================================

const PORT = process.env.SIGNALING_PORT || 3001;
server.listen(PORT, () => {
    console.log(`\n========================================`);
    console.log(`  MeetPro Signaling Server`);
    console.log(`  Port: ${PORT}`);
    console.log(`  Transport: WebSocket + Polling`);
    console.log(`  State: In-Memory (no database)`);
    console.log(`========================================\n`);
});
