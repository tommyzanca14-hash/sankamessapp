const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mongoose = require('mongoose');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static(__dirname));

// --- CONNESSIONE A MONGODB E MODELLO MESSAGGI ---
const mongoUri = process.env.MONGODB_URI;
if (mongoUri) {
    mongoose.connect(mongoUri)
        .then(() => console.log('Connesso a MongoDB Atlas con successo!'))
        .catch(err => console.error('Errore di connessione a MongoDB:', err));
}

const messageSchema = new mongoose.Schema({
    senderPhone: String,
    receiverPhone: String,
    text: String,
    timestamp: { type: Date, default: Date.now }
});

const Message = mongoose.model('Message', messageSchema);
// -----------------------------------------------

// Mappa globale in memoria per tracciare i numeri di telefono e i loro socket attivi
const users = {}; // Mappa phone -> socket.id

io.on('connection', (socket) => {
    console.log('Nuova connessione socket stabilita:', socket.id);

    // Registrazione utente
    socket.on('register_user', (userData) => {
        if (!userData || !userData.phone) return;
        
        // Associa il numero al socket ID corrente
        users[userData.phone] = socket.id;
        socket.phone = userData.phone;
        
        console.log(`[REGISTRAZIONE] Utente associato: ${userData.name} (${userData.phone}) -> Socket ID: ${socket.id}`);
        socket.emit('registration_success', userData);
        
        // Consegna i messaggi offline accumulati
        deliverOfflineMessages(userData.phone, socket);
    });

    // Login utente
    socket.on('login_user', (phone) => {
        if (!phone) return;
        
        // Associa il numero al socket ID corrente (sovrascrive eventuali vecchie sessioni)
        users[phone] = socket.id;
        socket.phone = phone;
        
        console.log(`[LOGIN] Utente online con numero: ${phone} -> Socket ID: ${socket.id}`);
        socket.emit('login_success', phone);
        
        // Consegna i messaggi offline accumulati
        deliverOfflineMessages(phone, socket);
    });

    // Invio messaggio
    socket.on('send_message', async (msg) => {
        if (!msg || !msg.senderPhone || !msg.receiverPhone || !msg.text) return;
        
        console.log(`[MESSAGGIO] Da ${msg.senderPhone} a ${msg.receiverPhone}: ${msg.text}`);
        
        // 1. Cerchiamo il socket ID aggiornato del destinatario nella mappa
        const receiverSocketId = users[msg.receiverPhone];
        
        // 2. Salviamo SEMPRE il messaggio su MongoDB
        try {
            const newMessage = new Message({
                senderPhone: msg.senderPhone,
                receiverPhone: msg.receiverPhone,
                text: msg.text
            });
            await newMessage.save();
            console.log(`[DB] Messaggio salvato correttamente nel database.`);
        } catch (err) {
            console.error('[ERRORE DB] Impossibile salvare il messaggio:', err);
        }

        // 3. Se il destinatario è online, inviamoglielo SUBITO in tempo reale
        if (receiverSocketId) {
            io.to(receiverSocketId).emit('receive_message', msg);
            console.log(`[REALTIME] Consegnato in tempo reale al socket ${receiverSocketId} (${msg.receiverPhone})`);
        } else {
            console.log(`[OFFLINE] Il destinatario ${msg.receiverPhone} non è attualmente nella mappa degli utenti online.`);
        }
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
        if (socket.phone) {
            console.log(`[DISCONNESSO] Utente ${socket.phone} con socket ${socket.id}`);
            // Rimuoviamo dalla mappa solo se il socket disconnesso corrisponde all'ultimo attivo
            if (users[socket.phone] === socket.id) {
                delete users[socket.phone];
            }
        }
    });
});

async function deliverOfflineMessages(phone, socket) {
    try {
        const pendingMessages = await Message.find({ receiverPhone: phone }).sort({ timestamp: 1 });
        
        if (pendingMessages && pendingMessages.length > 0) {
            console.log(`[OFFLINE SYNC] Trovati ${pendingMessages.length} messaggi offline per ${phone}`);
            
            for (const msg of pendingMessages) {
                socket.emit('receive_message', {
                    senderPhone: msg.senderPhone,
                    receiverPhone: msg.receiverPhone,
                    text: msg.text,
                    timestamp: msg.timestamp
                });
            }
            
            // Rimuoviamo i messaggi dal DB dopo averli consegnati
            await Message.deleteMany({ receiverPhone: phone });
            console.log(`[OFFLINE SYNC] Messaggi consegnati e rimossi dal DB per ${phone}`);
        }
    } catch (err) {
        console.error('[ERRORE OFFLINE SYNC]:', err);
    }
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Server avviato sulla porta ${PORT}`);
});
