var express = require('express');
var app = express();
var bodyParser = require('body-parser');
var request = require('request');
var exec = require('child_process').exec;

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

var newlyAdded = 0;

// get recommendation api

app.get('/getRec', function(req, res){
  request({
    url: 'http://localhost:8000/linkrec/' + req.query.user + '/rec:results',
    method: 'GET', 
  }, function (error, response, body){
    if (error) {
      res.status(response.statusCode).send(response.statusMessage);
    } else {
      var result = {};
      if (response.statusCode == 200) result = JSON.parse(body);

      console.log('[Output] user:' + req.query.user, result);
      res.json(result);
    }
  });
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

function writeToDB(args) {
  // for input, only one user and a list of links
  // { user: id, links: [ { url: url, title: name, time: timestamp }, ... ] }
  
  console.log('[Input]', args.input);

  var data = {'Row': [{}]};

  var user = new Buffer(args.input.user).toString('base64');
  var row = data.Row[0];

  row.key = user;
  row.Cell = [];

  var links = args.input.links;
  var linkNum = links.length;

  for (var i = 0; i < linkNum; i++) {
    
    var column = new Buffer('link:' + escape(links[i].url)).toString('base64');
    var value = new Buffer(escape(links[i].title)).toString('base64');
    var timestamp = links[i].time;
    
    var cell = {'timestamp': timestamp, 'column': column, '$': value};
    row.Cell.push(cell);

  }

  console.log('[Request Data]', JSON.stringify(data));

  request({
    url: 'http://localhost:8000/linkrec/falserow',
    method: 'POST',
    json: true,
    body: data
  }, function (error, response, body){
    if (!error) {
      args.success(response.statusCode);
    } else if (args.error) {
      args.error(response.statusCode, response.statusMessage);
    } else {
      console.error(response);
    }
  });
}

app.post('/sendLink', function (req, res) {
  writeToDB({
    input: req.body,
    success: function (statusCode) {
      newlyAdded++;
      res.status(statusCode).end();
    },
    error: function (statusCode, statusMessage) {
      res.status(statusCode).send(statusMessage);
    }
  });
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
    console.log('[Spark] Training data...')
    var temp = newlyAdded;
    exec(command, function (error, stdout, stderr) {
      if (!error) {
        newlyAdded -= temp;
        console.log('[Spark] Training completed')
      } else {
        console.log(error);
        console.log(stdout);
        console.log(stderr);
      }
    });
  } else {
    console.log('[Spark] No newly added data')
  }
}

setInterval(trainData, 300000); // 5min
