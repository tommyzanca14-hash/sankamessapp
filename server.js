const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const path = require('path');
const fs = require('fs');

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname)));

const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: "*", methods: ["GET", "POST"] }
});

const DB_FILE = path.join(__dirname, 'database.json');

function loadData() {
    if (fs.existsSync(DB_FILE)) {
        try {
            return JSON.parse(fs.readFileSync(DB_FILE, 'utf8'));
        } catch (e) {
            return { users: [], contacts: [], messages: [] };
        }
    }
    return { users: [], contacts: [], messages: [] };
}

function saveData(data) {
    fs.writeFileSync(DB_FILE, JSON.stringify(data, null, 2));
}

io.on('connection', (socket) => {
    console.log('Un utente si è connesso:', socket.id);

    socket.on('register_user', (userData) => {
        let db = loadData();
        let existingUser = db.users.find(u => u.phone === userData.phone);
        if (!existingUser) {
            db.users.push(userData);
            saveData(db);
        }
        socket.emit('registration_success', userData);
    });

    socket.on('add_contact', (contactData) => {
        let db = loadData();
        db.contacts.push(contactData);
        saveData(db);

        const userContacts = db.contacts.filter(c => c.ownerPhone === contactData.ownerPhone);
        socket.emit('update_contacts', userContacts);
    });

    socket.on('send_message', (msgData) => {
        let db = loadData();
        db.messages.push(msgData);
        saveData(db);
        io.emit('receive_message', msgData);
    });

    socket.on('get_initial_data', (userPhone) => {
        let db = loadData();
        const userContacts = db.contacts.filter(c => c.ownerPhone === userPhone);
        socket.emit('initial_data', { contacts: userContacts, messages: db.messages });
    });

    // --- Gestione Segnalazione WebRTC per Chiamate Audio/Video ---
    socket.on('call_user', (data) => {
        io.emit('incoming_call', {
            fromPhone: data.fromPhone,
            fromName: data.fromName,
            toPhone: data.toPhone,
            signal: data.signal,
            callType: data.callType // 'audio' o 'video'
        });
    });

    socket.on('answer_call', (data) => {
        io.emit('call_accepted', {
            toPhone: data.toPhone,
            signal: data.signal
        });
    });

    socket.on('ice_candidate', (data) => {
        io.emit('ice_candidate', {
            toPhone: data.toPhone,
            signal: data.signal
        });
    });

    socket.on('hang_up', (data) => {
        io.emit('call_ended', { toPhone: data.toPhone });
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Server avviato sulla porta ${PORT}`);
});
