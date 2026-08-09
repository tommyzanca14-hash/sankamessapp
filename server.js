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

// Modelli Database
const userSchema = new mongoose.Schema({
    name: { type: String, required: true, trim: true },
    phone: { type: String, required: true, unique: true, trim: true },
    email: { type: String, required: true, trim: true }
});
const User = mongoose.model('User', userSchema);

const messageSchema = new mongoose.Schema({
    sender: String,
    recipient: String,
    isGroup: { type: Boolean, default: false },
    text: String,
    status: { type: String, default: 'sent' },
    timestamp: { type: Date, default: Date.now }
});
const Message = mongoose.model('Message', messageSchema);

const groupSchema = new mongoose.Schema({
    name: { type: String, required: true },
    description: { type: String, default: '' },
    admin: { type: String, required: true },
    members: [{ type: String }]
});
const Group = mongoose.model('Group', groupSchema);

const callLogSchema = new mongoose.Schema({
    callType: String,
    isGroup: { type: Boolean, default: false },
    initiatorPhone: String,
    initiatorName: String,
    participants: [{ phone: String, name: String }],
    status: { type: String, default: 'pending' },
    timestamp: { type: Date, default: Date.now }
});
const CallLog = mongoose.model('CallLog', callLogSchema);

const activeUsers = new Map(); // phone -> Set di socket.id

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
                user = new User({ name: cleanName, phone: cleanPhone, email: cleanEmail });
                await user.save();
            } else {
                user.name = cleanName;
                user.email = cleanEmail;
                await user.save();
            }

            if (!activeUsers.has(user.phone)) {
                activeUsers.set(user.phone, new Set());
            }
            activeUsers.get(user.phone).add(socket.id);
            socket.userPhone = user.phone;

            socket.emit('registration_success', user);

            const pending = await Message.find({ recipient: user.phone, status: 'sent', isGroup: false });
            for (let msg of pending) {
                msg.status = 'delivered';
                await msg.save();
                socket.emit('receive_message', msg);
                
                const senderSockets = activeUsers.get(msg.sender);
                if (senderSockets && senderSockets.size > 0) {
                    senderSockets.forEach(sId => {
                        io.to(sId).emit('message_status_update', { messageId: msg._id, status: 'delivered' });
                    });
                }
            }
        } catch (err) {
            console.error("Errore registrazione:", err);
            socket.emit('registration_error', 'Errore del server o numero già registrato.');
        }
    });

    socket.on('login_user', async (data) => {
        try {
            if (!data || !data.phone) return;
            let user = await User.findOne({ phone: data.phone.trim() });
            if (user) {
                if (!activeUsers.has(user.phone)) {
                    activeUsers.set(user.phone, new Set());
                }
                activeUsers.get(user.phone).add(socket.id);
                socket.userPhone = user.phone;

                socket.emit('login_success', user);

                const userGroups = await Group.find({ members: user.phone });
                userGroups.forEach(g => socket.join(g._id.toString()));

                const pending = await Message.find({ recipient: user.phone, status: 'sent', isGroup: false });
                for (let msg of pending) {
                    msg.status = 'delivered';
                    await msg.save();
                    socket.emit('receive_message', msg);

                    const senderSockets = activeUsers.get(msg.sender);
                    if (senderSockets && senderSockets.size > 0) {
                        senderSockets.forEach(sId => {
                            io.to(sId).emit('message_status_update', { messageId: msg._id, status: 'delivered' });
                        });
                    }
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

    socket.on('get_user_details', async (data, callback) => {
        try {
            const user = await User.findOne({ phone: data.phone });
            if (user) {
                callback({ success: true, user: { name: user.name, phone: user.phone, email: user.email } });
            } else {
                callback({ success: false });
            }
        } catch (err) {
            callback({ success: false });
        }
    });

    socket.on('create_group', async (data, callback) => {
        try {
            const { name, description, members, admin } = data;
            if (!name || !members || members.length === 0) {
                return callback({ success: false, message: "Dati gruppo non validi." });
            }

            if (!members.includes(admin)) {
                members.push(admin);
            }

            const newGroup = new Group({
                name: name.trim(),
                description: description ? description.trim() : '',
                admin: admin,
                members: members
            });

            await newGroup.save();

            members.forEach(phone => {
                const sSockets = activeUsers.get(phone);
                if (sSockets) {
                    sSockets.forEach(sId => {
                        const targetSocket = io.sockets.sockets.get(sId);
                        if (targetSocket) targetSocket.join(newGroup._id.toString());
                    });
                }
            });

            io.to(newGroup._id.toString()).emit('group_created', newGroup);
            callback({ success: true, group: newGroup });
        } catch (err) {
            console.error("Errore creazione gruppo:", err);
            callback({ success: false, message: "Errore interno del server." });
        }
    });

    socket.on('get_user_groups', async (data) => {
        try {
            const groups = await Group.find({ members: data.userPhone });
            groups.forEach(g => socket.join(g._id.toString()));
            socket.emit('user_groups_list', groups);
        } catch (err) {
            console.error("Errore recupero gruppi:", err);
        }
    });

    // --- INVIO MESSAGGIO OTTIMIZZATO IN PARALLELO ---
    socket.on('send_message', async (msgData, callback) => {
        try {
            const { sender, recipient, text, isGroup, tempId } = msgData;

            const messageId = new mongoose.Types.ObjectId();
            const timestamp = new Date();
            
            const recipientSockets = !isGroup ? activeUsers.get(recipient) : null;
            const isOnline = !isGroup && recipientSockets && recipientSockets.size > 0;

            let initialStatus = (isGroup || isOnline) ? 'delivered' : 'sent';

            const messagePayload = {
                id: messageId,
                sender: sender,
                recipient: recipient,
                isGroup: !!isGroup,
                text: text,
                status: initialStatus,
                timestamp: timestamp
            };

            // 1. SPEDIZIONE ISTANTANEA VIA WEBSOCKET (Zero attese)
            if (isGroup) {
                io.to(recipient).emit('receive_message', messagePayload);
            } else if (isOnline) {
                recipientSockets.forEach(sId => {
                    io.to(sId).emit('receive_message', messagePayload);
                });
            }

            // 2. RISPOSTA IMMEDIATA AL MITTENTE
            if (typeof callback === 'function') {
                callback({ success: true, message: messagePayload, tempId });
            }

            // 3. SALVATAGGIO SU MONGODB IN BACKGROUND (Non blocca la consegna)
            const newMessage = new Message({
                _id: messageId,
                sender,
                recipient,
                isGroup: !!isGroup,
                text,
                status: initialStatus,
                timestamp
            });
            
            newMessage.save().catch(dbErr => {
                console.error("Errore salvataggio asincrono su MongoDB:", dbErr);
            });

        } catch (err) {
            console.error("Errore invio messaggio:", err);
            if (typeof callback === 'function') callback({ success: false });
        }
    });

    socket.on('mark_messages_read', async (data) => {
        try {
            const { userPhone, chatPartnerId, isGroup } = data;
            let query = isGroup ? { recipient: chatPartnerId, isGroup: true } : { sender: chatPartnerId, recipient: userPhone, isGroup: false };

            await Message.updateMany({ ...query, status: { $ne: 'read' } }, { $set: { status: 'read' } });

            if (!isGroup) {
                const senderSockets = activeUsers.get(chatPartnerId);
                if (senderSockets && senderSockets.size > 0) {
                    senderSockets.forEach(sId => {
                        io.to(sId).emit('messages_read_receipt', { readerPhone: userPhone });
                    });
                }
            }
        } catch (err) {
            console.error("Errore aggiornamento stato lettura:", err);
        }
    });

    socket.on('get_chat_history', async (data) => {
        try {
            const { user1, user2, isGroup } = data;
            let history = [];
            if (isGroup) {
                history = await Message.find({ recipient: user2, isGroup: true }).sort({ timestamp: 1 });
            } else {
                history = await Message.find({
                    isGroup: false,
                    $or: [
                        { sender: user1, recipient: user2 },
                        { sender: user2, recipient: user1 }
                    ]
                }).sort({ timestamp: 1 });
            }
            socket.emit('chat_history', history);
        } catch (err) {
            console.error("Errore cronologia:", err);
            socket.emit('chat_history', []);
        }
    });

    socket.on('get_all_chats_history', async (data) => {
        try {
            const { userPhone } = data;
            const userGroups = await Group.find({ members: userPhone });
            const groupIds = userGroups.map(g => g._id.toString());

            const history = await Message.find({
                $or: [
                    { sender: userPhone },
                    { recipient: userPhone },
                    { recipient: { $in: groupIds }, isGroup: true }
                ]
            }).sort({ timestamp: 1 });

            socket.emit('all_chats_history', history);
        } catch (err) {
            console.error("Errore cronologia globale:", err);
            socket.emit('all_chats_history', []);
        }
    });

    // --- GESTIONE CHIAMATE WEBRTC ---
    socket.on('check_user_availability', async (data, callback) => {
        const targetPhone = data.targetPhone;
        const targetSockets = activeUsers.get(targetPhone);
        const isOnline = targetSockets && targetSockets.size > 0;
        if (typeof callback === 'function') {
            callback({ online: !!isOnline });
        }
    });

    socket.on('start_call', async (data) => {
        try {
            let participantsList = [{ phone: data.initiatorPhone, name: data.initiatorName }];
            let validTargets = [];

            if (data.targets && data.targets.length > 0) {
                for (let targetPhone of data.targets) {
                    const targetSockets = activeUsers.get(targetPhone);
                    const isOnline = targetSockets && targetSockets.size > 0;
                    if (isOnline) {
                        validTargets.push(targetPhone);
                    }
                }
            }

            if (validTargets.length === 0) {
                const callLog = new CallLog({
                    callType: data.callType,
                    isGroup: data.isGroup,
                    initiatorPhone: data.initiatorPhone,
                    initiatorName: data.initiatorName,
                    participants: participantsList,
                    status: 'unreachable'
                });
                await callLog.save();
                socket.emit('call_unreachable', { message: 'Nessun utente raggiungibile o online.' });
                return;
            }

            if (data.targets && data.targets.length > 0) {
                const targetUsers = await User.find({ phone: { $in: data.targets } });
                targetUsers.forEach(t => {
                    if (!participantsList.some(p => p.phone === t.phone)) {
                        participantsList.push({ phone: t.phone, name: t.name });
                    }
                });
            }

            const callLog = new CallLog({
                callType: data.callType,
                isGroup: data.isGroup,
                initiatorPhone: data.initiatorPhone,
                initiatorName: data.initiatorName,
                participants: participantsList,
                status: 'pending'
            });
            await callLog.save();

            validTargets.forEach(targetPhone => {
                const targetSockets = activeUsers.get(targetPhone);
                if (targetSockets) {
                    targetSockets.forEach(sId => {
                        io.to(sId).emit('incoming_call', {
                            callId: callLog._id,
                            initiatorPhone: data.initiatorPhone,
                            initiatorName: data.initiatorName,
                            callType: data.callType,
                            isGroup: data.isGroup,
                            groupId: data.groupId,
                            targets: data.targets
                        });
                    });
                }
            });

            socket.emit('call_initiated', { callId: callLog._id });
        } catch (err) {
            console.error("Errore avvio chiamata:", err);
        }
    });

    socket.on('call_accepted', async (data) => {
        try {
            await CallLog.findByIdAndUpdate(data.callId, { status: 'completed' });
            io.emit('call_peer_joined', { callId: data.callId, userPhone: data.userPhone });
        } catch (err) {
            console.error("Errore accettazione chiamata:", err);
        }
    });

    socket.on('join_call', async (data) => {
        try {
            await CallLog.findByIdAndUpdate(data.callId, {
                status: 'completed',
                $addToSet: { participants: { phone: data.userPhone, name: data.userName } }
            });

            if (data.toIdentifier) {
                const recipientSockets = activeUsers.get(data.toIdentifier);
                if (recipientSockets) {
                    recipientSockets.forEach(sId => {
                        io.to(sId).emit('call_signal', {
                            fromPhone: data.userPhone,
                            signal: data.signal
                        });
                    });
                }
            }
        } catch (err) {
            console.error("Errore unione chiamata:", err);
        }
    });

    socket.on('call_signal', (data) => {
        const recipientSockets = activeUsers.get(data.toIdentifier);
        if (recipientSockets) {
            recipientSockets.forEach(sId => {
                io.to(sId).emit('call_signal', {
                    fromPhone: data.fromPhone,
                    signal: data.signal
                });
            });
        }
    });

    socket.on('call_declined', async (data) => {
        if (data && data.callId) {
            await CallLog.findByIdAndUpdate(data.callId, { status: 'declined' });
        }
        if (data && data.toIdentifier) {
            const recipientSockets = activeUsers.get(data.toIdentifier);
            if (recipientSockets) {
                recipientSockets.forEach(sId => {
                    io.to(sId).emit('call_declined', { fromPhone: data.fromPhone, userName: data.userName });
                });
            }
        }
    });

    socket.on('leave_call', (data) => {
        if (data && data.callId && data.userPhone) {
            io.emit('call_participant_left', { callId: data.callId, userPhone: data.userPhone });
        }
    });

    socket.on('end_call_for_all', async (data) => {
        if (data && data.callId) {
            await CallLog.findByIdAndUpdate(data.callId, { status: 'completed' });
        }
        if (data && data.targets) {
            data.targets.forEach(phone => {
                const sSockets = activeUsers.get(phone);
                if (sSockets) {
                    sSockets.forEach(sId => {
                        io.to(sId).emit('call_ended');
                    });
                }
            });
        }
        io.emit('call_ended');
    });

    socket.on('get_call_logs', async (data) => {
        try {
            const logs = await CallLog.find({
                $or: [
                    { initiatorPhone: data.userPhone },
                    { "participants.phone": data.userPhone }
                ]
            }).sort({ timestamp: -1 }).limit(50);
            socket.emit('call_logs_list', logs);
        } catch (err) {
            console.error("Errore recupero registro chiamate:", err);
        }
    });

    socket.on('disconnect', async () => {
        if (socket.userPhone) {
            const userSockets = activeUsers.get(socket.userPhone);
            if (userSockets) {
                userSockets.delete(socket.id);
                if (userSockets.size === 0) {
                    activeUsers.delete(socket.userPhone);
                    await CallLog.updateMany(
                        { status: 'pending', $or: [{ initiatorPhone: socket.userPhone }, { "participants.phone": socket.userPhone }] },
                        { $set: { status: 'missed' } }
                    );
                }
            }
        }
        console.log('Utente disconnesso:', socket.id);
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Server avviato sulla porta ${PORT}`);
});
