const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mongoose = require('mongoose');
const cors = require('cors');
const path = require('path');

const app = express();
app.use(cors());
app.use(express.json());

app.use(express.static(path.join(__dirname)));

const server = http.createServer(app);
const io = new Server(server, {
    cors: {
        origin: "*",
        methods: ["GET", "POST"]
    }
});

const MONGO_URI = process.env.MONGO_URI;

if (!MONGO_URI) {
    console.error("ATTENZIONE: La variabile d'ambiente MONGO_URI non è impostata correttamente!");
}

mongoose.connect(MONGO_URI || "mongodb://localhost:27017/omega7", {
    serverSelectionTimeoutMS: 5000
})
    .then(() => console.log("Connesso a MongoDB Atlas con successo"))
    .catch(err => console.error("Errore di connessione a MongoDB:", err));

const userSchema = new mongoose.Schema({
    name: { type: String, required: true, trim: true },
    phone: { type: String, required: true, unique: true, trim: true },
    email: { type: String, required: true, trim: true }
});
const User = mongoose.model('User', userSchema);

const messageSchema = new mongoose.Schema({
    sender: String,
    recipient: String,
    text: String,
    status: { type: String, default: 'sent' },
    timestamp: { type: Date, default: Date.now }
});
const Message = mongoose.model('Message', messageSchema);

const activeUsers = new Map();

io.on('connection', (socket) => {
    console.log('Un utente si è connesso:', socket.id);

    socket.on('register_user', async (data) => {
        try {
            if (!data || !data.name || !data.phone) {
                socket.emit('registration_error', 'Inserisci nome e telefono.');
                return;
            }

            let cleanPhone = data.phone.trim();
            let cleanEmail = data.email ? data.email.trim() : "noemail@omega.com";
            let cleanName = data.name.trim();

            let user = await User.findOne({ phone: cleanPhone });
            if (!user) {
                user = new User({
                    name: cleanName,
                    phone: cleanPhone,
                    email: cleanEmail
                });
                await user.save();
            } else {
                // Aggiorna i dati se l'utente esiste già ma fa una nuova registrazione pulita
                user.name = cleanName;
                user.email = cleanEmail;
                await user.save();
            }

            activeUsers.set(user.phone, socket.id);
            socket.userPhone = user.phone;
            socket.emit('registration_success', user);

            const pending = await Message.find({ recipient: user.phone, status: 'sent' });
            for (let msg of pending) {
                socket.emit('receive_message', msg);
                msg.status = 'delivered';
                await msg.save();
            }
        } catch (err) {
            console.error("Errore registrazione:", err);
            socket.emit('registration_error', 'Errore del server o numero già registrato.');
        }
    });

    socket.on('login_user', async (data) => {
        try {
            if(!data || !data.phone) return;
            let user = await User.findOne({ phone: data.phone.trim() });
            if (user) {
                activeUsers.set(user.phone, socket.id);
                socket.userPhone = user.phone;
                socket.emit('login_success', user);

                const pending = await Message.find({ recipient: user.phone, status: 'sent' });
                for (let msg of pending) {
                    socket.emit('receive_message', msg);
                    msg.status = 'delivered';
                    await msg.save();
                }
            } else {
                socket.emit('registration_error', 'Utente non trovato.');
            }
        } catch (err) {
            console.error("Errore login:", err);
        }
    });

    socket.on('lookup_contact', async (query, callback) => {
        try {
            const cleanQuery = query.trim();
            const foundUser = await User.findOne({
                $or: [
                    { phone: cleanQuery },
                    { name: { $regex: new RegExp(`^${cleanQuery}$`, 'i') } },
                    { email: { $regex: new RegExp(`^${cleanQuery}$`, 'i') } }
                ]
            });

            if (foundUser) {
                callback({ success: true, user: foundUser });
            } else {
                callback({ success: false, message: "Utente non registrato nel sistema." });
            }
        } catch (err) {
            console.error("Errore lookup:", err);
            callback({ success: false, message: "Errore del server." });
        }
    });

    socket.on('send_message', async (msgData, callback) => {
        try {
            let recipientSocketId = activeUsers.get(msgData.recipient);
            const isOnline = recipientSocketId && io.sockets.sockets.has(recipientSocketId);

            const newMessage = new Message({
                sender: msgData.sender,
                recipient: msgData.recipient,
                text: msgData.text,
                status: isOnline ? 'delivered' : 'sent'
            });

            await newMessage.save();

            const messagePayload = {
                id: newMessage._id,
                sender: newMessage.sender,
                recipient: newMessage.recipient,
                text: newMessage.text,
                status: newMessage.status,
                timestamp: newMessage.timestamp
            };

            if (isOnline) {
                io.to(recipientSocketId).emit('receive_message', messagePayload);
            }
            
            if(typeof callback === 'function') {
                callback({ success: true, message: messagePayload, tempId: msgData.tempId });
            }

        } catch (err) {
            console.error("Errore invio messaggio:", err);
            if(typeof callback === 'function') {
                callback({ success: false });
            }
        }
    });

    socket.on('get_chat_history', async (data) => {
        try {
            const { user1, user2 } = data;
            const history = await Message.find({
                $or: [
                    { sender: user1, recipient: user2 },
                    { sender: user2, recipient: user1 }
                ]
            }).sort({ timestamp: 1 });

            socket.emit('chat_history', history);
        } catch (err) {
            console.error("Errore cronologia:", err);
            socket.emit('chat_history', []);
        }
    });

    socket.on('get_all_chats_history', async (data) => {
        try {
            const { userPhone } = data;
            const history = await Message.find({
                $or: [
                    { sender: userPhone },
                    { recipient: userPhone }
                ]
            }).sort({ timestamp: 1 });

            socket.emit('all_chats_history', history);
        } catch (err) {
            console.error("Errore cronologia globale:", err);
            socket.emit('all_chats_history', []);
        }
    });

    socket.on('update_status', async (data) => {
        try {
            const updatedMsg = await Message.findByIdAndUpdate(data.messageId, { status: data.status }, { new: true });
            if (updatedMsg) {
                // Notifica sia il mittente che il destinatario per aggiornare le spunte blu in tempo reale
                let senderSocketId = activeUsers.get(updatedMsg.sender);
                let recipientSocketId = activeUsers.get(updatedMsg.recipient);
                
                const payload = { messageId: data.messageId, status: data.status };
                if (senderSocketId) io.to(senderSocketId).emit('message_status_updated', payload);
                if (recipientSocketId) io.to(recipientSocketId).emit('message_status_updated', payload);
            }
        } catch (err) {
            console.error("Errore aggiornamento stato:", err);
        }
    });

    socket.on('call_user', (data) => {
        const recipientSocketId = activeUsers.get(data.toIdentifier);
        if (recipientSocketId && io.sockets.sockets.has(recipientSocketId)) {
            io.to(recipientSocketId).emit('incoming_call', {
                fromPhone: data.fromPhone,
                fromName: data.fromName,
                signal: data.signal,
                callType: data.callType
            });
        } else {
            socket.emit('call_failed', { reason: "L'utente non è raggiungibile o è offline." });
        }
    });

    socket.on('answer_call', (data) => {
        const recipientSocketId = activeUsers.get(data.toIdentifier);
        if (recipientSocketId) {
            io.to(recipientSocketId).emit('call_accepted', { signal: data.signal });
        }
    });

    socket.on('ice_candidate', (data) => {
        const recipientSocketId = activeUsers.get(data.toIdentifier);
        if (recipientSocketId) {
            io.to(recipientSocketId).emit('ice_candidate', { signal: data.signal });
        }
    });

    socket.on('hang_up', (data) => {
        const recipientSocketId = activeUsers.get(data.toIdentifier);
        if (recipientSocketId) {
            io.to(recipientSocketId).emit('call_ended');
        }
    });

    socket.on('disconnect', () => {
        if (socket.userPhone) {
            activeUsers.delete(socket.userPhone);
        }
        console.log('Utente disconnesso:', socket.id);
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Server avviato sulla porta ${PORT}`);
});
