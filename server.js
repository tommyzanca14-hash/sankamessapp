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

// --- CONNESSIONE A MONGODB ATLAS ---
const mongoUri = process.env.MONGODB_URI;
if (mongoUri) {
    mongoose.connect(mongoUri)
        .then(() => console.log('Connesso a MongoDB Atlas con successo!'))
        .catch(err => console.error('Errore di connessione a MongoDB:', err));
} else {
    console.warn('ATTENZIONE: MONGODB_URI non impostato nelle variabili d\'ambiente!');
}

const messageSchema = new mongoose.Schema({
    senderPhone: String,
    receiverPhone: String,
    text: String,
    timestamp: { type: Date, default: Date.now }
});

const Message = mongoose.model('Message', messageSchema);
// -----------------------------------

// Mappa in memoria per associare il numero di telefono al socket ID attivo
const activeUsers = {}; // phone -> socket.id

io.on('connection', (socket) => {
    console.log(`[CONNESSIONE] Nuovo client connesso: ${socket.id}`);

    // Registrazione o Login utente
    socket.on('register_user', (userData) => {
        if (!userData || !userData.phone) return;
        activeUsers[userData.phone] = socket.id;
        socket.phone = userData.phone;
        console.log(`[UTENTE ONLINE] ${userData.name} (${userData.phone}) -> Socket: ${socket.id}`);
        socket.emit('registration_success', userData);
        deliverPendingMessages(userData.phone, socket);
    });

    socket.on('login_user', (phone) => {
        if (!phone) return;
        activeUsers[phone] = socket.id;
        socket.phone = phone;
        console.log(`[UTENTE LOGIN] Telefono: ${phone} -> Socket: ${socket.id}`);
        socket.emit('login_success', phone);
        deliverPendingMessages(phone, socket);
    });

    // Gestione Messaggi
    socket.on('send_message', async (msg) => {
        if (!msg || !msg.senderPhone || !msg.receiverPhone || !msg.text) return;

        console.log(`[MESSAGGIO] Da ${msg.senderPhone} a ${msg.receiverPhone}: ${msg.text}`);

        // 1. Salvataggio obbligatorio su MongoDB
        try {
            const newMsg = new Message({
                senderPhone: msg.senderPhone,
                receiverPhone: msg.receiverPhone,
                text: msg.text
            });
            await newMsg.save();
            console.log(`[DB] Messaggio salvato con successo nel database.`);
        } catch (err) {
            console.error('[ERRORE DB] Impossibile salvare il messaggio:', err);
        }

        // 2. Controllo se il destinatario è online e invio in tempo reale
        const receiverSocketId = activeUsers[msg.receiverPhone];
        if (receiverSocketId) {
            io.to(receiverSocketId).emit('receive_message', msg);
            console.log(`[REALTIME] Consegnato in tempo reale al socket ${receiverSocketId}`);
        } else {
            console.log(`[OFFLINE] Il destinatario ${msg.receiverPhone} non è online. Il messaggio rimarrà nel DB.`);
        }
    });

    // --- Segnalazione WebRTC (Chiamate e Videochiamate) ---
    socket.on('call_user', (data) => {
        const targetSocket = activeUsers[data.toPhone];
        if (targetSocket) {
            io.to(targetSocket).emit('incoming_call', data);
        } else {
            socket.emit('call_failed', { reason: "L'utente chiamato non è al momento online." });
        }
    });

    socket.on('answer_call', (data) => {
        const targetSocket = activeUsers[data.toPhone];
        if (targetSocket) {
            io.to(targetSocket).emit('call_accepted', data);
        }
    });

    socket.on('ice_candidate', (data) => {
        const targetSocket = activeUsers[data.toPhone];
        if (targetSocket) {
            io.to(targetSocket).emit('ice_candidate', data);
        }
    });

    socket.on('hang_up', (data) => {
        const targetSocket = activeUsers[data.toPhone];
        if (targetSocket) {
            io.to(targetSocket).emit('call_ended', data);
        }
    });

    // Disconnessione
    socket.on('disconnect', () => {
        if (socket.phone && activeUsers[socket.phone] === socket.id) {
            delete activeUsers[socket.phone];
            console.log(`[DISCONNESSO] Utente con telefono ${socket.phone} disconnesso.`);
        }
    });
});

// Funzione per consegnare i messaggi accumulati quando l'utente era offline
async function deliverPendingMessages(phone, socket) {
    try {
        const pending = await Message.find({ receiverPhone: phone }).sort({ timestamp: 1 });
        if (pending && pending.length > 0) {
            console.log(`[SYNC OFFLINE] Trovati ${pending.length} messaggi pendenti per ${phone}`);
            for (const m of pending) {
                socket.emit('receive_message', {
                    senderPhone: m.senderPhone,
                    receiverPhone: m.receiverPhone,
                    text: m.text,
                    time: new Date(m.timestamp).getTime()
                });
            }
            // Pulizia dal database dopo la consegna
            await Message.deleteMany({ receiverPhone: phone });
            console.log(`[SYNC OFFLINE] Messaggi pendenti consegnati e rimossi dal DB.`);
        }
    } catch (err) {
        console.error('[ERRORE SYNC OFFLINE]:', err);
    }
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Server avviato e in ascolto sulla porta ${PORT}`);
});
