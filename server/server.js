var express = require('express');
var app = express();
var bodyParser = require('body-parser');
var request = require('request');
var exec = require('child_process').exec;

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));



// run recommendation api

app.get('/getRec', function(req, res){

  var command = '/usr/local/spark/bin/spark-submit --class org.linkourmet.LinkourmetALS --jars /usr/local/hbase/lib/hbase-protocol-1.0.0.jar,/usr/local/hbase/lib/hbase-common-1.0.0.jar,/usr/local/hbase/lib/hbase-client-1.0.0.jar,/usr/local/hbase/lib/zookeeper-3.4.6.jar,/usr/local/hbase/lib/guava-12.0.1.jar,/usr/local/hbase/lib/protobuf-java-2.5.0.jar,/usr/local/hbase/lib/htrace-core-3.1.0-incubating.jar,/usr/local/hbase/lib/hbase-server-1.0.0.jar ~/target/scala-2.10/linkourmet_2.10-1.0.jar ' + req.query.user; 

  exec(command, function (error, stdout, stderr) {
    res.json(JSON.parse(stdout));
  });

});


// send link api

function writeToDB(input) {
  // for input, only one user and a list of links
  // { user: id, links: [ { url: url, title: name, time: timestamp }, ... ] }
  
  var data = {'Row': [{}]};

  var user = new Buffer(input.user).toString('base64');
  var row = data.Row[0];

  row.key = user;
  row.Cell = [];

  var links = input.links;
  var linkNum = links.length;

  for (var i = 0; i < linkNum; i++) {
    
    var column = new Buffer('link:' + links[i].url).toString('base64');
    var value = new Buffer(links[i].title).toString('base64');
    var timestamp = links[i].time;
    
    var cell = {'timestamp': timestamp, 'column': column, '$': value};
    row.Cell.push(cell);

  }

  var status = {'code': 200, 'message': ''};

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
