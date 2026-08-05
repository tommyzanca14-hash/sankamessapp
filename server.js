const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mongoose = require('mongoose');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: {
        origin: "*",
        methods: ["GET", "POST"]
    }
});

// Serve i file statici dalla cartella principale
app.use(express.static(__dirname));

// Gestione esplicita della root per servire index.html correttamente
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'index.html'));
});

// --- CONNESSIONE A MONGODB ATLAS & MODELLI ---
const mongoUri = process.env.MONGODB_URI;
if (mongoUri) {
    mongoose.connect(mongoUri)
        .then(() => console.log('Connesso a MongoDB Atlas con successo!'))
        .catch(err => console.error('Errore di connessione a MongoDB:', err));
} else {
    console.warn('ATTENZIONE: MONGODB_URI non impostato nelle variabili d\'ambiente!');
}

const userSchema = new mongoose.Schema({
    name: { type: String, unique: true, required: true },
    phone: { type: String, unique: true, required: true },
    email: { type: String, unique: true, required: true }
});
const User = mongoose.model('User', userSchema);

const messageSchema = new mongoose.Schema({
    sender: String,
    recipient: String,
    text: String,
    status: { type: String, default: 'sent' }, // 'sent', 'delivered', 'read'
    timestamp: { type: Date, default: Date.now }
});
const Message = mongoose.model('Message', messageSchema);
// ---------------------------------------------

// Mappa in memoria estesa per associare qualsiasi identificativo al socket ID
const activeUsers = {}; // identifier -> socket.id

function registerActiveUser(identifier, socketId) {
    if (!identifier) return;
    const cleanId = String(identifier).trim();
    activeUsers[cleanId] = socketId;
    // Registra anche versioni pulite da spazi o prefissi comuni se necessario
    const numericOnly = cleanId.replace(/\D/g, '');
    if (numericOnly) activeUsers[numericOnly] = socketId;
}

io.on('connection', (socket) => {
    console.log(`[CONNESSIONE] Nuovo client connesso: ${socket.id}`);

    // Registrazione utente
    socket.on('register_user', async (userData) => {
        if (!userData || !userData.phone || !userData.name || !userData.email) return;
        try {
            let existing = await User.findOne({ $or: [{ name: userData.name }, { phone: userData.phone }, { email: userData.email }] });
            if (existing) {
                socket.emit('registration_error', 'Username, telefono o email già registrati nel sistema.');
                return;
            }
            const newUser = new User(userData);
            await newUser.save();

            registerActiveUser(userData.phone, socket.id);
            registerActiveUser(userData.name, socket.id);
            registerActiveUser(userData.email, socket.id);
            socket.identifier = userData.phone;

            socket.emit('registration_success', userData);
            deliverPendingMessages(userData, socket);
        } catch(err) {
            console.error('[ERRORE REGISTRAZIONE]', err);
            socket.emit('registration_error', 'Errore durante la registrazione.');
        }
    });

    // Login utente
    socket.on('login_user', async (userData) => {
        let phone = typeof userData === 'string' ? userData : userData.phone;
        let name = typeof userData === 'object' ? userData.name : null;
        let email = typeof userData === 'object' ? userData.email : null;

        if (!phone) return;
        
        registerActiveUser(phone, socket.id);
        if(name) registerActiveUser(name, socket.id);
        if(email) registerActiveUser(email, socket.id);
        socket.identifier = phone;

        console.log(`[UTENTE LOGIN] Identificativo: ${phone} -> Socket: ${socket.id}`);
        socket.emit('login_success', phone);
        
        deliverPendingMessages({ phone, name, email }, socket);
    });

    // Richiesta della cronologia messaggi tra due utenti (per garantire che siano eterni e visibili)
    socket.on('get_chat_history', async ({ user1, user2 }) => {
        try {
            const history = await Message.find({
                $or: [
                    { sender: user1, recipient: user2 },
                    { sender: user2, recipient: user1 }
                ]
            }).sort({ timestamp: 1 });

            socket.emit('chat_history', history);
        } catch (err) {
            console.error('[ERRORE HISTORY]', err);
        }
    });

    // Gestione Invio Messaggi e Spunte
    socket.on('send_message', async (msg) => {
        if (!msg || !msg.sender || !msg.recipient || !msg.text) return;

        console.log(`[MESSAGGIO] Da ${msg.sender} a ${msg.recipient}: ${msg.text}`);

        // Cerca il destinatario usando varie chiavi possibili (numero esatto, pulito, nome)
        let recipientSocketId = activeUsers[msg.recipient] || activeUsers[String(msg.recipient).trim()];
        if (!recipientSocketId) {
            const numeric = String(msg.recipient).replace(/\D/g, '');
            recipientSocketId = activeUsers[numeric];
        }

        let initialStatus = recipientSocketId ? 'delivered' : 'sent';

        try {
            const newMsg = new Message({
                sender: msg.sender,
                recipient: msg.recipient,
                text: msg.text,
                status: initialStatus
            });
            await newMsg.save();
            
            msg.id = newMsg._id.toString();
            msg.status = initialStatus;
            msg.timestamp = newMsg.timestamp;

            if (recipientSocketId) {
                io.to(recipientSocketId).emit('receive_message', msg);
                console.log(`[REALTIME] Consegnato in tempo reale al socket ${recipientSocketId}`);
            } else {
                console.log(`[OFFLINE] Destinatario ${msg.recipient} non trovato online. Salvato su DB.`);
            }

            socket.emit('message_status_updated', { messageId: msg.id, status: initialStatus });

        } catch (err) {
            console.error('[ERRORE DB] Impossibile salvare il messaggio:', err);
        }
    });

    // Aggiornamento stato messaggio (es. 'read')
    socket.on('update_status', async (data) => {
        if (!data || !data.messageId || !data.status) return;
        try {
            let updated = await Message.findByIdAndUpdate(data.messageId, { status: data.status }, { new: true });
            if (updated) {
                let senderSocketId = activeUsers[updated.sender] || activeUsers[String(updated.sender).trim()];
                if (senderSocketId) {
                    io.to(senderSocketId).emit('message_status_updated', { messageId: data.messageId, status: data.status });
                }
            }
        } catch(e) {
            console.error('[ERRORE AGGIORNAMENTO STATO]', e);
        }
    });

    // WebRTC Chiamate
    socket.on('call_user', (data) => {
        const targetSocket = activeUsers[data.toIdentifier] || activeUsers[String(data.toIdentifier).trim()];
        if (targetSocket) {
            io.to(targetSocket).emit('incoming_call', data);
        } else {
            socket.emit('call_failed', { reason: "L'utente chiamato non è al momento online." });
        }
    });

    socket.on('answer_call', (data) => {
        const targetSocket = activeUsers[data.toIdentifier];
        if (targetSocket) io.to(targetSocket).emit('call_accepted', data);
    });

    socket.on('ice_candidate', (data) => {
        const targetSocket = activeUsers[data.toIdentifier];
        if (targetSocket) io.to(targetSocket).emit('ice_candidate', data);
    });

    socket.on('hang_up', (data) => {
        const targetSocket = activeUsers[data.toIdentifier];
        if (targetSocket) io.to(targetSocket).emit('call_ended', data);
    });

    // Disconnessione
    socket.on('disconnect', () => {
        if (socket.identifier) {
            delete activeUsers[socket.identifier];
            console.log(`[DISCONNESSO] Socket disconnesso: ${socket.id}`);
        }
    });
});

async function deliverPendingMessages(userData, socket) {
    try {
        let queryCriteria = [ { recipient: userData.phone } ];
        if(userData.name) queryCriteria.push({ recipient: userData.name });
        if(userData.email) queryCriteria.push({ recipient: userData.email });

        const pending = await Message.find({ $or: queryCriteria, status: { $ne: 'read' } }).sort({ timestamp: 1 });
        if (pending && pending.length > 0) {
            for (const m of pending) {
                if(m.status === 'sent') {
                    m.status = 'delivered';
                    await m.save();
                    
                    let senderSocket = activeUsers[m.sender];
                    if(senderSocket) {
                        io.to(senderSocket).emit('message_status_updated', { messageId: m._id.toString(), status: 'delivered' });
                    }
                }

                socket.emit('receive_message', {
                    id: m._id.toString(),
                    sender: m.sender,
                    recipient: m.recipient,
                    text: m.text,
                    status: m.status,
                    time: new Date(m.timestamp).getTime()
                });
            }
        }
    } catch (err) {
        console.error('[ERRORE SYNC OFFLINE]:', err);
    }
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Server avviato e in ascolto sulla porta ${PORT}`);
});
