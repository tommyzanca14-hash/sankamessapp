const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const path = require('path');

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname)));

const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: "*", methods: ["GET", "POST"] }
});

let userSockets = {};
let offlineMessages = {}; // { phone: [ {senderPhone, receiverPhone, text, timestamp} ] }

io.on('connection', (socket) => {
    console.log('Un utente si è connesso:', socket.id);

    socket.on('register_user', (userData) => {
        userSockets[userData.phone] = socket.id;
        socket.emit('registration_success', userData);
        deliverOfflineMessages(userData.phone, socket);
    });

    socket.on('login_user', (phone) => {
        userSockets[phone] = socket.id;
        socket.emit('login_success', phone);
        deliverOfflineMessages(phone, socket);
    });

    socket.on('send_message', (msgData) => {
        let targetSocketId = userSockets[msgData.receiverPhone];
        
        if (targetSocketId) {
            // Se l'utente è online, invia subito
            io.to(targetSocketId).emit('receive_message', msgData);
        } else {
            // Se è offline, salviamo in coda
            if (!offlineMessages[msgData.receiverPhone]) {
                offlineMessages[msgData.receiverPhone] = [];
            }
            offlineMessages[msgData.receiverPhone].push(msgData);
            console.log(`Messaggio messo in coda per l'utente offline: ${msgData.receiverPhone}`);
        }
        
        // Invia conferma anche al mittente
        socket.emit('receive_message', msgData);
    });

    function deliverOfflineMessages(phone, socket) {
        if (offlineMessages[phone] && offlineMessages[phone].length > 0) {
            console.log(`Consegna di ${offlineMessages[phone].length} messaggi offline a ${phone}`);
            offlineMessages[phone].forEach(msg => {
                socket.emit('receive_message', msg);
            });
            delete offlineMessages[phone]; // Svuota la coda dopo la consegna
        }
    }

    // --- Gestione WebRTC (Chiamate e Videochiamate) ---
    socket.on('call_user', (data) => {
        let targetSocketId = userSockets[data.toPhone];
        if (targetSocketId) {
            io.to(targetSocketId).emit('incoming_call', {
                fromPhone: data.fromPhone,
                fromName: data.fromName,
                toPhone: data.toPhone,
                signal: data.signal,
                callType: data.callType
            });
        } else {
            socket.emit('call_failed', { reason: "L'utente non è online o irraggiungibile." });
        }
    });

    socket.on('answer_call', (data) => {
        let targetSocketId = userSockets[data.toPhone];
        if (targetSocketId) {
            io.to(targetSocketId).emit('call_accepted', {
                signal: data.signal
            });
        }
    });

    socket.on('ice_candidate', (data) => {
        let targetSocketId = userSockets[data.toPhone];
        if (targetSocketId) {
            io.to(targetSocketId).emit('ice_candidate', {
                signal: data.signal
            });
        }
    });

    socket.on('hang_up', (data) => {
        let targetSocketId = userSockets[data.toPhone];
        if (targetSocketId) {
            io.to(targetSocketId).emit('call_ended');
        }
    });

    socket.on('disconnect', () => {
        for (let phone in userSockets) {
            if (userSockets[phone] === socket.id) {
                delete userSockets[phone];
                break;
            }
        }
        console.log('Utente disconnesso:', socket.id);
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Server avviato sulla porta ${PORT}`);
});
