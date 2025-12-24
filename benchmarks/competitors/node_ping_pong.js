const net = require('net');

const ITERATIONS = 500000;
const PING = "PING\n";

// Server
const server = net.createServer((socket) => {
    socket.pipe(socket); // Echo
});

server.listen(3132, '127.0.0.1', () => {
    // Client
    const client = new net.Socket();
    let pongs = 0;
    let start = 0;

    client.connect(3132, '127.0.0.1', () => {
        start = process.hrtime.bigint();
        client.write(PING);
    });

    client.on('data', (data) => {
        pongs++;
        if (pongs >= ITERATIONS) {
            const end = process.hrtime.bigint();
            const nanos = Number(end - start);
            const seconds = nanos / 1e9;
            const rps = ITERATIONS / seconds;
            
            console.log(`Node.js: ${rps.toFixed(2)} roundtrips/s`);
            console.log(`Time: ${seconds.toFixed(2)}s`);
            
            client.destroy();
            server.close();
        } else {
            client.write(PING);
        }
    });
});

