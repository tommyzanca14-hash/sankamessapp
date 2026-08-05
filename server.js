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

const MONGO_URI = process.env.MONGO_URI || "mongodb://localhost:27017/omega7";

mongoose.connect(MONGO_URI)
    .then(() => console.log("Connesso a MongoDB con successo"))
    .catch(err => console.error("Errore di connessione a MongoDB:", err));

const userSchema = new mongoose.Schema({
    name: { type: String, required: true },
    phone: { type: String, required: true, unique: true },
    email: { type: String, required: true }
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

            let user = await User.findOne({ phone: data.phone });
            if (!user) {
                user = new User({
                    name: data.name.trim(),
                    phone: data.phone.trim(),
                    email: data.email ? data.email.trim() : "noemail@omega.com"
                });
                await user.save();
            }

            activeUsers.set(data.phone, socket.id);
            socket.userPhone = data.phone;
            socket.emit('registration_success', user);

            // Invia messaggi pendenti offline
            const pending = await Message.find({ recipient: data.phone, status: 'sent' });
            for (let msg of pending) {
                socket.emit('receive_message', msg);
                msg.status = 'delivered';
                await msg.save();
            }
        } catch (err) {
            console.error("Errore registrazione:", err);
            socket.emit('registration_error', 'Errore del server.');
        }
    });

    socket.on('login_user', async (data) => {
        try {
            let user = await User.findOne({ phone: data.phone });
            if (user) {
                activeUsers.set(data.phone, socket.id);
                socket.userPhone = data.phone;
                socket.emit('login_success', user);

                const pending = await Message.find({ recipient: data.phone, status: 'sent' });
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

    // Invio messaggi robusto: garantisce che il messaggio venga mostrato e non sparisca
    socket.on('send_message', async (msgData) => {
        try {
            const recipientSocketId = activeUsers.get(msgData.recipient);
            const isOnline = recipientSocketId && io.sockets.sockets.has(recipientSocketId);

            const newMessage = new Message({
                sender: msgData.sender,
                recipient: msgData.recipient,
                text: msgData.text,
                status: isOnline ? 'delivered' : 'sent'
            });

            await newMessage.save();

            const responsePayload = {
                id: newMessage._id,
                sender: newMessage.sender,
                recipient: newMessage.recipient,
                text: newMessage.text,
                status: newMessage.status,
                timestamp: newMessage.timestamp
            };

            // Spedisci al destinatario se è online
            if (isOnline) {
                io.to(recipientSocketId).emit('receive_message', responsePayload);
            }

            // Spedisci anche al mittente per confermare e mostrare il messaggio in chat senza attese
            socket.emit('receive_message', responsePayload);

        } catch (err) {
            console.error("Errore invio messaggio:", err);
            socket.emit('message_error', 'Impossibile inviare il messaggio.');
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
