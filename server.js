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

// Mappe in memoria
const users = {};          // Mappa phone -> socket.id

io.on('connection', (socket) => {
    console.log('Un utente si è connesso:', socket.id);

    // Registrazione o Login unificato
    socket.on('register_user', (userData) => {
        if (!userData || !userData.phone) return;
        users[userData.phone] = socket.id;
        socket.phone = userData.phone;
        console.log(`Utente registrato/connesso: ${userData.name} (${userData.phone})`);
        socket.emit('registration_success', userData);
        
        // Invia eventuali messaggi offline dal DB
        deliverOfflineMessages(userData.phone, socket);
    });

    socket.on('login_user', (phone) => {
        if (!phone) return;
        users[phone] = socket.id;
        socket.phone = phone;
        console.log(`Login utente con numero: ${phone}`);
        socket.emit('login_success', phone);
        
        // Invia eventuali messaggi offline dal DB
        deliverOfflineMessages(phone, socket);
    });

    socket.on('send_message', async (msg) => {
        if (!msg || !msg.senderPhone || !msg.receiverPhone) return;
        
        console.log(`Messaggio da ${msg.senderPhone} a ${msg.receiverPhone}: ${msg.text}`);
        const receiverSocketId = users[msg.receiverPhone];
        
        // 1. SALVIAMO SUBITO SU MONGODB (Garantisce la persistenza certa)
        try {
            const newMessage = new Message({
                senderPhone: msg.senderPhone,
                receiverPhone: msg.receiverPhone,
                text: msg.text
            });
            await newMessage.save();
        } catch (err) {
            console.error('Errore nel salvataggio del messaggio su MongoDB:', err);
        }

        // 2. SE IL DESTINATARIO È ONLINE, INVIO IMMEDIATO
        if (receiverSocketId) {
            io.to(receiverSocketId).emit('receive_message', msg);
            console.log(`Messaggio consegnato in tempo reale a ${msg.receiverPhone}`);
        } else {
            console.log(`Destinatario ${msg.receiverPhone} offline. Messaggio salvato nel database.`);
        }
        
        // 3. Rimbalza sempre al mittente per confermare l'invio grafico
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

async function deliverOfflineMessages(phone, socket) {
    try {
        // Cerca tutti i messaggi destinati a questo numero ordinati dal più vecchio al più recente
        const pendingMessages = await Message.find({ receiverPhone: phone }).sort({ timestamp: 1 });
        
        if (pendingMessages && pendingMessages.length > 0) {
            console.log(`Invio di ${pendingMessages.length} messaggi offline dal DB a ${phone}`);
            
            for (const msg of pendingMessages) {
                socket.emit('receive_message', {
                    senderPhone: msg.senderPhone,
                    receiverPhone: msg.receiverPhone,
                    text: msg.text
                });
            }
            
            // Rimuoviamo i messaggi consegnati dal DB solo dopo averli inviati tutti con successo
            await Message.deleteMany({ receiverPhone: phone });
        }
    } catch (err) {
        console.error('Errore nel recupero dei messaggi offline:', err);
    }
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Server avviato sulla porta ${PORT}`);
});
