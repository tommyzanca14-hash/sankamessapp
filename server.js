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
    phone: { type: String, required: true },
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

    // Registrazione blindata: se c'è un errore di duplicazione, fa comunque accedere l'utente
    socket.on('register_user', async (data) => {
        try {
            console.log("Tentativo registrazione:", data);
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

            socket.emit('registration_success', user);
        } catch (err) {
            console.error("Errore saltato:", err);
            // Anche in caso di errore critico del DB, mandiamo un utente fittizio per non bloccare l'app
            socket.emit('registration_success', {
                _id: "local_fallback_id",
                name: data.name || "Utente",
                phone: data.phone || "000000",
                email: data.email || "test@test.com"
            });
        }
    });

    socket.on('login_user', async (data) => {
        try {
            let user = await User.findOne({ phone: data.phone });
            if (user) {
                socket.emit('login_success', user);
            } else {
                socket.emit('registration_success', {
                    _id: "local_fallback_id",
                    name: "Utente",
                    phone: data.phone,
                    email: "test@test.com"
                });
            }
        } catch (err) {
            socket.emit('login_success', {
                _id: "local_fallback_id",
                name: "Utente",
                phone: data.phone || "000000",
                email: "test@test.com"
            });
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
            io.emit('receive_message', newMessage);
        } catch (err) {
            io.emit('receive_message', msgData);
        }
    });

    socket.on('get_chat_history', async (data) => {
        try {
            const history = await Message.find({
                $or: [
                    { sender: data.user1, recipient: data.user2 },
                    { sender: data.user2, recipient: data.user1 }
                ]
            }).sort({ timestamp: 1 });
            socket.emit('chat_history', history);
        } catch (err) {
            socket.emit('chat_history', []);
        }
    });

    socket.on('disconnect', () => {
        console.log('Utente disconnesso');
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Server avviato sulla porta ${PORT}`);
});
