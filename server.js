const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mongoose = require('mongoose');
const cors = require('cors');
const path = require('path');

const app = express();
app.use(cors());
app.use(express.json());

// Serve i file statici (come index.html) dalla cartella principale
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
    name: { type: String, unique: true, required: true },
    phone: { type: String, unique: true, required: true },
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

io.on('connection', (socket) => {
    console.log('Un utente si è connesso:', socket.id);

    socket.on('register_user', async (data) => {
        try {
            if (!data.name || !data.phone || !emailValid(data.email)) {
                socket.emit('registration_error', 'Dati non validi o incompleti.');
                return;
            }

            let existingUser = await User.findOne({ 
                $or: [{ phone: data.phone }, { name: data.name }] 
            });

            if (existingUser) {
                socket.emit('registration_success', existingUser);
                return;
            }

            const newUser = new User({
                name: data.name.trim(),
                phone: data.phone.trim(),
                email: data.email.trim()
            });

            await newUser.save();
            socket.emit('registration_success', newUser);
        } catch (err) {
            console.error("Errore durante la registrazione:", err);
            socket.emit('registration_error', 'Errore del server durante la registrazione.');
        }
    });

    socket.on('login_user', async (data) => {
        try {
            if (!data.phone) return;
            let user = await User.findOne({ phone: data.phone });
            if (user) {
                socket.emit('login_success', user);
            } else {
                socket.emit('registration_error', 'Utente non trovato. Registrati prima.');
            }
        } catch (err) {
            console.error("Errore durante il login:", err);
        }
    });

    socket.on('send_message', async (msgData) => {
        try {
            const newMessage = new Message({
                sender: msgData.sender,
                recipient: msgData.recipient,
                text: msgData.text,
                status: 'delivered'
            });

            await newMessage.save();

            io.emit('receive_message', {
                id: newMessage._id,
                sender: newMessage.sender,
                recipient: newMessage.recipient,
                text: newMessage.text,
                status: newMessage.status,
                timestamp: newMessage.timestamp
            });
        } catch (err) {
            console.error("Errore invio messaggio:", err);
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
            console.error("Errore recupero cronologia:", err);
        }
    });

    socket.on('update_status', async (data) => {
        try {
            await Message.findByIdAndUpdate(data.messageId, { status: data.status });
            io.emit('message_status_updated', { messageId: data.messageId, status: data.status });
        } catch (err) {
            console.error("Errore aggiornamento stato:", err);
        }
    });

    socket.on('call_user', (data) => {
        socket.broadcast.emit('incoming_call', data);
    });

    socket.on('answer_call', (data) => {
        socket.broadcast.emit('call_accepted', data);
    });

    socket.on('ice_candidate', (data) => {
        socket.broadcast.emit('ice_candidate', data);
    });

    socket.on('hang_up', (data) => {
        socket.broadcast.emit('call_ended', data);
    });

    socket.on('disconnect', () => {
        console.log('Utente disconnesso:', socket.id);
    });
});

function emailValid(email) {
    return email && email.includes('@');
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Server avviato sulla porta ${PORT}`);
});
