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

// Mappa per associare il numero di telefono al socket.id attivo
let userSockets = {};

io.on('connection', (socket) => {
    console.log('Un utente si è connesso:', socket.id);

    socket.on('register_user', (userData) => {
        let db = loadData();
        let existingUser = db.users.find(u => u.phone === userData.phone);
        if (!existingUser) {
            db.users.push(userData);
            saveData(db);
        }
        userSockets[userData.phone] = socket.id;
        socket.emit('registration_success', userData);
    });

    // Registra il numero anche al caricamento iniziale se l'utente ha già fatto login
    socket.on('get_initial_data', (userPhone) => {
        userSockets[userPhone] = socket.id;
        let db = loadData();
        const userContacts = db.contacts.filter(c => c.ownerPhone === userPhone);
        socket.emit('initial_data', { contacts: userContacts, messages: db.messages });
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

    // --- Gestione Segnalazione WebRTC Mirata per Numero ---
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
