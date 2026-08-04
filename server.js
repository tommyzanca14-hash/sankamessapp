const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json());

app.get('/', (req, res) => {
  res.send('Ciao! La mia app funziona!');
});

const server = http.createServer(app);
const io = new Server(server, {
    cors: {
        origin: "*",
        methods: ["GET", "POST"]
    }
});

let users = []; 
let messages = []; 
let posts = []; 

io.on('connection', (socket) => {
    console.log('Un utente si è connesso:', socket.id);

    socket.on('register_user', (userData) => {
        users.push(userData);
        socket.emit('registration_success', { message: 'Registrazione avvenuta con successo!' });
    });

    socket.on('send_message', (data) => {
        messages.push(data);
        io.emit('receive_message', data);
    });

    socket.on('new_post', (postData) => {
        posts.unshift(postData);
        io.emit('update_feed', posts);
    });

    socket.on('disconnect', () => {
        console.log('Utente disconnesso:', socket.id);
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Server avviato sulla porta ${PORT}`);
});
