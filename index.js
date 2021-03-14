const express = require('express');
const bodyParser = require('body-parser')
const cors = require('cors')
const stream = require('./controllers/stream')

const app = express();
const port = 4000;

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

app.use(cors());
app.options('*', cors()) 
app.post('*', cors()) 
app.use('/stream',stream);

app.listen(port, ()=>   {
    console.log("App listening on port",port)
})