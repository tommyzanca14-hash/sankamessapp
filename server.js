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

const server = http.createServer(server ? server : app); // Compatibilità server
const io = new Server(http.createServer(app) ? http.createServer(app) : server, {
    cors: { origin: "*", methods: ["GET", "POST"] }
});

// File locale per salvare i dati in modo persistente su Render
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

    // Registrazione utente
    socket.on('register_user', (userData) => {
        let db = loadData();
        let existingUser = db.users.find(u => u.phone === userData.phone);
        if (!existingUser) {
            db.users.push(userData);
            saveData(db);
        }
        socket.emit('registration_success', userData);
    });

    // Aggiunta contatto
    socket.on('add_contact', (contactData) => {
        let db = loadData();
        db.contacts.push(contactData);
        saveData(db);
        io.emit('update_contacts', db.contacts);
    });

    // Invio messaggio
    socket.on('send_message', (msgData) => {
        let db = loadData();
        db.messages.push(msgData);
        saveData(db);
        io.emit('receive_message', msgData);
    });

    // Richiesta dati iniziali
    socket.on('get_initial_data', () => {
        let db = loadData();
        socket.emit('initial_data', { contacts: db.contacts, messages: db.messages });
    });
});

// Avvia il server sulla porta assegnata da Render o 3000
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Server avviato sulla porta ${PORT}`);
});
     
