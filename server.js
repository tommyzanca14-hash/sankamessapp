const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static(__dirname));

// Mappe per utenti online e messaggi offline in attesa
const users = {};
const offlineMessages = {}; // { phoneNumber: [messages...] }

io.on('connection', (socket) => {
    console.log('Un utente si è connesso:', socket.id);

    // Registrazione dell'utente tramite numero di telefono
    socket.on('register_user', (userData) => {
        users[userData.phone] = socket.id;
        socket.phone = userData.phone;
        console.log(`Utente registrato: ${userData.name} (${userData.phone}) -> Socket ID: ${socket.id}`);
        socket.emit('registration_success', userData);
        
        // Consegna eventuali messaggi offline accumulati
        if (offlineMessages[userData.phone] && offlineMessages[userData.phone].length > 0) {
            offlineMessages[userData.phone].forEach(msg => {
                socket.emit('receive_message', msg);
            });
            offlineMessages[userData.phone] = []; // Svuota la coda dopo l'invio
        }
    });

    socket.on('login_user', (phone) => {
        users[phone] = socket.id;
        socket.phone = phone;
        console.log(`Login utente con numero: ${phone} -> Socket ID: ${socket.id}`);
        socket.emit('login_success', phone);
        
        // Consegna eventuali messaggi offline accumulati al login
        if (offlineMessages[phone] && offlineMessages[phone].length > 0) {
            offlineMessages[phone].forEach(msg => {
                socket.emit('receive_message', msg);
            });
            offlineMessages[phone] = [];
        }
    });

    // Gestione messaggi con supporto offline completo
    socket.on('send_message', (msg) => {
        console.log(`Messaggio da ${msg.senderPhone} a ${msg.receiverPhone}: ${msg.text}`);
        const receiverSocketId = users[msg.receiverPhone];
        
        if (receiverSocketId) {
            // Destinatario online: invia subito
            io.to(receiverSocketId).emit('receive_message', msg);
        } else {
            // Destinatario offline: salva in memoria temporanea
            if (!offlineMessages[msg.receiverPhone]) {
                offlineMessages[msg.receiverPhone] = [];
            }
            offlineMessages[msg.receiverPhone].push(msg);
            console.log(`Destinatario ${msg.receiverPhone} offline. Messaggio salvato in coda.`);
        }
        
        // Rimbalza sempre al mittente per la sincronizzazione della chat locale
        socket.emit('receive_message', msg);
    });

    // --- Segnalazione WebRTC per Chiamate e Videochiamate ---
    socket.on('call_user', (data) => {
        const calleeSocketId = users[data.toPhone];
        if (calleeSocketId) {
            io.to(calleeSocketId).emit('incoming_call', data);
        } else {
            socket.emit('call_failed', { reason: "L'utente chiamato non è online o irraggiungibile." });
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

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Server avviato sulla porta ${PORT}`);
});
