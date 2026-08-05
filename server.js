const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const fs = require('fs');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static(__dirname));

const USERS_FILE = path.join(__dirname, 'users.json');
const MESSAGES_FILE = path.join(__dirname, 'offline_messages.json');

// Funzioni di utilità per leggere e scrivere su file JSON
function loadData(filePath) {
    try {
        if (fs.existsSync(filePath)) {
            const data = fs.readFileSync(filePath, 'utf8');
            return JSON.parse(data);
        }
    } catch (e) {
        console.error("Errore lettura file:", e);
    }
    return {};
}

function saveData(filePath, data) {
    try {
        fs.writeFileSync(filePath, JSON.stringify(data, null, 2), 'utf8');
    } catch (e) {
        console.error("Errore scrittura file:", e);
    }
}

// Mappe in memoria per gli utenti connessi
const users = {}; 
// Struttura offlineMessages salvata su file: { phoneNumber: [messages...] }
let offlineMessages = loadData(MESSAGES_FILE);

io.on('connection', (socket) => {
    console.log('Un utente si è connesso:', socket.id);

    socket.on('register_user', (userData) => {
        users[userData.phone] = socket.id;
        socket.phone = userData.phone;
        console.log(`Utente registrato: ${userData.name} (${userData.phone})`);
        socket.emit('registration_success', userData);
        
        // Consegna messaggi offline salvati permanentemente
        if (offlineMessages[userData.phone] && offlineMessages[userData.phone].length > 0) {
            offlineMessages[userData.phone].forEach(msg => {
                socket.emit('receive_message', msg);
            });
            offlineMessages[userData.phone] = [];
            saveData(MESSAGES_FILE, offlineMessages);
        }
    });

    socket.on('login_user', (phone) => {
        users[phone] = socket.id;
        socket.phone = phone;
        console.log(`Login utente con numero: ${phone}`);
        socket.emit('login_success', phone);
        
        // Consegna messaggi offline al login
        if (offlineMessages[phone] && offlineMessages[phone].length > 0) {
            offlineMessages[phone].forEach(msg => {
                socket.emit('receive_message', msg);
            });
            offlineMessages[phone] = [];
            saveData(MESSAGES_FILE, offlineMessages);
        }
    });

    socket.on('send_message', (msg) => {
        console.log(`Messaggio da ${msg.senderPhone} a ${msg.receiverPhone}: ${msg.text}`);
        const receiverSocketId = users[msg.receiverPhone];
        
        if (receiverSocketId) {
            // Destinatario online
            io.to(receiverSocketId).emit('receive_message', msg);
        } else {
            // Destinatario offline: salvataggio su file permanente
            if (!offlineMessages[msg.receiverPhone]) {
                offlineMessages[msg.receiverPhone] = [];
            }
            offlineMessages[msg.receiverPhone].push(msg);
            saveData(MESSAGES_FILE, offlineMessages);
            console.log(`Destinatario ${msg.receiverPhone} offline. Salvato su file.`);
        }
        
        // Rimbalza al mittente
        socket.emit('receive_message', msg);
    });

    // --- Segnalazione WebRTC per Chiamate ---
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
