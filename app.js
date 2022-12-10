require("dotenv").config({ path: "./.env", override: true });

const http = require('http');
const { connect, getCollection } = require("./mongo");

const hostname = '127.0.0.1';
const port = parseInt(process.env.PORT) || 4000;

const server = http.createServer((req, res) => {
  res.statusCode = 200;
  res.setHeader('Content-Type', 'text/plain');
  res.end('Hello, World!\n');
});

connect().then(() => {
  getCollection("Operators").findOne({}).then(console.log);
  server.listen(port, hostname, () => {
    console.log(`Server running at ${hostname}:${port}`);
  });
})