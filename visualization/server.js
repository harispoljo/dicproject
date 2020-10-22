



const express = require('express')
const cassandra = require('cassandra-driver');
const cors = require('cors')

const app = express();
const PORT = process.env.PORT || 8080;

app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(cors());

const client = new cassandra.Client({
  contactPoints: ['127.0.0.1'],
  localDataCenter: 'datacenter1',
  keyspace: 'covid_space'
});


app.get("/api/data", (req, res) => {
      var query = 'SELECT * from covid';
      client.execute(query)
        .then(post => {
          res.json(200, post)
          console.log(post)});
});

app.listen(PORT, console.log(`Server starting at ${PORT}`))
