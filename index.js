const express = require('express');
const bodyParser = require('body-parser')
const stream = require('./controllers/stream')

const app = express();
const port = 3000;

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

app.use('/stream',stream);
app.listen(port, ()=>   {
    console.log("App listening on port ${port}")
})