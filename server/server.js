var express = require('express');
var app = express();
var bodyParser = require('body-parser');
var request = require('request');
var exec = require('child_process').exec;

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

var newlyAdded = 0;

// run recommendation api

function getRecommendation(user) {
  // TODO read from hbase
}

app.get('/getRec', function(req, res){
  res.json(getRecommendation(req.query.user));
});


// send link api

function escape(str) {
  return str
    .replace(/[\\]/g, '\\\\')
    .replace(/[\"]/g, '\\\"')
    .replace(/[\/]/g, '\\/')
    .replace(/[\b]/g, '\\b')
    .replace(/[\f]/g, '\\f')
    .replace(/[\n]/g, '\\n')
    .replace(/[\r]/g, '\\r')
    .replace(/[\t]/g, '\\t');
}

function writeToDB(input) {
  // for input, only one user and a list of links
  // { user: id, links: [ { url: url, title: name, time: timestamp }, ... ] }
  
  console.log('[Input]');
  console.log(input);

  var data = {'Row': [{}]};

  var user = new Buffer(input.user).toString('base64');
  var row = data.Row[0];

  row.key = user;
  row.Cell = [];

  var links = input.links;
  var linkNum = links.length;

  for (var i = 0; i < linkNum; i++) {
    
    var column = new Buffer('link:' + escape(links[i].url)).toString('base64');
    var value = new Buffer(escape(links[i].title)).toString('base64');
    var timestamp = links[i].time;
    
    var cell = {'timestamp': timestamp, 'column': column, '$': value};
    row.Cell.push(cell);

  }

  var status = {'code': 200, 'message': ''};

  console.log('[Request Data]');
  console.log(JSON.stringify(data));

  request({
    url: 'http://localhost:8000/linkrec/falserow',
    method: 'POST',
    json: true,
    body: data
  }, function (error, response, body){
    if (error) {
      status.code = response.statusCode;
      status.message = response.statusMessage;
    }
  });
  
  return status;
}

app.post('/sendLink', function (req, res) {
  newlyAdded++;
  var status = writeToDB(req.body); //input
  if (status.code == 200) res.status(200).end();
  else res.status(status.code).send(status.message);
});


// reset table api

function createTable(schema, res) {
  request({
    url: 'http://localhost:8000/linkrec/schema',
    method: 'POST',
    json: true,
    body: schema
  }, function (error, response, body){
    if (!error) {
      console.log('[Reset] Table Reset');
      res.status(200).end();
    }
  });
}

function resetTable(schema, res) {
  request({
    url: 'http://localhost:8000/linkrec/schema',
    method: 'DELETE'
  }, function (error, response, body){
    if (!error) {
      createTable(schema, res);
    }
  });
}

app.delete('/reset', function (req, res) {
  request({
    url: 'http://localhost:8000/linkrec/schema',
    method: 'GET',
    json: true
  }, function (error, response, body){
    if (!error) {
      resetTable(body, res);
    }
  });
});

app.listen(3000);

var command = '/usr/local/spark-xc/bin/spark-submit --class LinkRec --master spark://spark:7077 --executor-memory 2g --total-executor-cores 6 ~/LinkRec/linkrec/target/scala-2.10/LinkRec-assembly-1.0.jar';

function trainData() {
  if (newlyAdded > 0) {
    var temp = newlyAdded;
    exec(command, function (error, stdout, stderr) {
      if (!error) {
        newlyAdded -= temp;
      }
    });
  }
}

setInterval(trainData, 300000);
