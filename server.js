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
    name: { type: String, unique: true, required: true }, // Username unico
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

// Mappa in memoria per associare identificativi (telefono, username, email) al socket ID attivo
const activeUsers = {}; // identifier -> socket.id

io.on('connection', (socket) => {
    console.log(`[CONNESSIONE] Nuovo client connesso: ${socket.id}`);

    // Registrazione utente con controllo unicità username
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

            activeUsers[userData.phone] = socket.id;
            activeUsers[userData.name] = socket.id;
            activeUsers[userData.email] = socket.id;
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
        activeUsers[phone] = socket.id;
        if(name) activeUsers[name] = socket.id;
        if(email) activeUsers[email] = socket.id;
        socket.identifier = phone;

        console.log(`[UTENTE LOGIN] Identificativo: ${phone} -> Socket: ${socket.id}`);
        socket.emit('login_success', phone);
        
        deliverPendingMessages({ phone, name, email }, socket);
    });

    // Gestione Invio Messaggi e Spunte
    socket.on('send_message', async (msg) => {
        if (!msg || !msg.sender || !msg.recipient || !msg.text) return;

        console.log(`[MESSAGGIO] Da ${msg.sender} a ${msg.recipient}: ${msg.text}`);

        let recipientSocketId = activeUsers[msg.recipient];
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

            if (recipientSocketId) {
                io.to(recipientSocketId).emit('receive_message', msg);
                console.log(`[REALTIME] Consegnato in tempo reale al socket ${recipientSocketId} con stato: ${initialStatus}`);
            } else {
                console.log(`[OFFLINE] Destinatario ${msg.recipient} offline. Messaggio salvato con spunta singola.`);
            }

            // Aggiorna subito il mittente con lo stato corretto (1 o 2 spunte bianche)
            socket.emit('message_status_updated', { messageId: msg.id, status: initialStatus });

        } catch (err) {
            console.error('[ERRORE DB] Impossibile salvare il messaggio:', err);
        }
    });

    // Aggiornamento stato messaggio (es. 'read' con spunte blu)
    socket.on('update_status', async (data) => {
        if (!data || !data.messageId || !data.status) return;
        try {
            let updated = await Message.findByIdAndUpdate(data.messageId, { status: data.status }, { new: true });
            if (updated) {
                let senderSocketId = activeUsers[updated.sender];
                if (senderSocketId) {
                    io.to(senderSocketId).emit('message_status_updated', { messageId: data.messageId, status: data.status });
                }
            }
        } catch(e) {
            console.error('[ERRORE AGGIORNAMENTO STATO]', e);
        }
    });

    // --- Segnalazione WebRTC (Chiamate e Videochiamate) ---
    socket.on('call_user', (data) => {
        const targetSocket = activeUsers[data.toIdentifier];
        if (targetSocket) {
            io.to(targetSocket).emit('incoming_call', data);
        } else {
            socket.emit('call_failed', { reason: "L'utente chiamato non è al momento online." });
        }
    });

    socket.on('answer_call', (data) => {
        const targetSocket = activeUsers[data.toIdentifier];
        if (targetSocket) {
            io.to(targetSocket).emit('call_accepted', data);
        }
    });

    socket.on('ice_candidate', (data) => {
        const targetSocket = activeUsers[data.toIdentifier];
        if (targetSocket) {
            io.to(targetSocket).emit('ice_candidate', data);
        }
    });

    socket.on('hang_up', (data) => {
        const targetSocket = activeUsers[data.toIdentifier];
        if (targetSocket) {
            io.to(targetSocket).emit('call_ended', data);
        }
    });

    // Disconnessione
    socket.on('disconnect', () => {
        if (socket.identifier) {
            delete activeUsers[socket.identifier];
            console.log(`[DISCONNESSO] Socket disconnesso: ${socket.id}`);
        }
    });
});

// Funzione per consegnare i messaggi pendenti offline e aggiornare a 'delivered' (2 spunte bianche)
async function deliverPendingMessages(userData, socket) {
    try {
        let queryCriteria = [ { recipient: userData.phone } ];
        if(userData.name) queryCriteria.push({ recipient: userData.name });
        if(userData.email) queryCriteria.push({ recipient: userData.email });

        const pending = await Message.find({ $or: queryCriteria, status: { $ne: 'read' } }).sort({ timestamp: 1 });
        if (pending && pending.length > 0) {
            console.log(`[SYNC OFFLINE] Trovati ${pending.length} messaggi pendenti per ${userData.phone}`);
            for (const m of pending) {
                // Se erano in stato 'sent', ora che è online diventano 'delivered'
                if(m.status === 'sent') {
                    m.status = 'delivered';
                    await m.save();
                    
                    // Notifica il mittente che il messaggio è ora consegnato (2 spunte bianche)
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
