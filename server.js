const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static(__dirname));

// Mappe in memoria
const users = {};          // Mappa phone -> socket.id
const offlineMessages = {}; // Mappa phone -> array di messaggi

io.on('connection', (socket) => {
    console.log('Un utente si è connesso:', socket.id);

    // Registrazione o Login unificato
    socket.on('register_user', (userData) => {
        users[userData.phone] = socket.id;
        socket.phone = userData.phone;
        console.log(`Utente registrato/connesso: ${userData.name} (${userData.phone})`);
        socket.emit('registration_success', userData);
        
        // Invia eventuali messaggi accumulati mentre era offline
        deliverOfflineMessages(userData.phone, socket);
    });

    socket.on('login_user', (phone) => {
        users[phone] = socket.id;
        socket.phone = phone;
        console.log(`Login utente con numero: ${phone}`);
        socket.emit('login_success', phone);
        
        // Invia eventuali messaggi accumulati mentre era offline
        deliverOfflineMessages(phone, socket);
    });

    socket.on('send_message', (msg) => {
        console.log(`Messaggio da ${msg.senderPhone} a ${msg.receiverPhone}: ${msg.text}`);
        const receiverSocketId = users[msg.receiverPhone];
        
        if (receiverSocketId) {
            // Destinatario online: invia subito
            io.to(receiverSocketId).emit('receive_message', msg);
            console.log(`Messaggio consegnato in tempo reale a ${msg.receiverPhone}`);
        } else {
            // Destinatario offline: salva in memoria
            if (!offlineMessages[msg.receiverPhone]) {
                offlineMessages[msg.receiverPhone] = [];
            }
            offlineMessages[msg.receiverPhone].push(msg);
            console.log(`Destinatario ${msg.receiverPhone} offline. Messaggio messo in coda.`);
        }
        
        // Rimbalza sempre il messaggio al mittente per confermare l'invio
        socket.emit('receive_message', msg);
    });

    // --- Segnalazione WebRTC per Chiamate ---
    socket.on('call_user', (data) => {
        const calleeSocketId = users[data.toPhone];
        if (calleeSocketId) {
            io.to(calleeSocketId).emit('incoming_call', data);
        } else {
            socket.emit('call_failed', { reason: "L'utente chiamato non è online." });
        }
    });

    socket.on('answer_call', (data) => {
        const callerSocketId = users[data.toPhone];
        if (callerSocketId) {
            io.to(callerSocketId).emit('call_accepted', data);
        }
    });

    socket.on('ice_candidate', (data) => {
        const targetSocketId = users[data.toPhone];
        if (targetSocketId) {
            io.to(targetSocketId).emit('ice_candidate', data);
        }
    });

    socket.on('hang_up', (data) => {
        const targetSocketId = users[data.toPhone];
        if (targetSocketId) {
            io.to(targetSocketId).emit('call_ended', data);
        }
    });

    socket.on('disconnect', () => {
        if (socket.phone && users[socket.phone] === socket.id) {
            delete users[socket.phone];
            console.log(`Utente con telefono ${socket.phone} disconnesso.`);
        }
    });
});

function deliverOfflineMessages(phone, socket) {
    if (offlineMessages[phone] && offlineMessages[phone].length > 0) {
        console.log(`Invio di ${offlineMessages[phone].length} messaggi offline a ${phone}`);
        offlineMessages[phone].forEach(msg => {
            socket.emit('receive_message', msg);
        });
        // Pulisce la coda una volta inviati
        offlineMessages[phone] = [];
    }
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Server avviato sulla porta ${PORT}`);
});
