//var amqp = require('amqplib/callback_api');
var stompit = require('stompit');
var ffmpeg = require('fluent-ffmpeg');
var async = require('async');
var mysql = require('mysql');
var config = require('./config');
var pool = mysql.createPool(config);

var work = async.queue(function(message, done) {
  ffmpeg(message.path)
    .on('end', function() {
      var inserts = [message.path, message.filename, message.number+'.jpg', message.modifiedDate];
      var sql = 'INSERT INTO video(path, name, thumbnail, modified_date) VALUES (?, ?, ?, FROM_UNIXTIME(?))';
      sql = mysql.format(sql, inserts);
      console.log(sql);
      pool.query(sql, function(error, results, fields) {
        console.log(error);
        done();
      });
    })
    .screenshots({
      timestamps: [ '25%' ],
      filename: message.number + '.jpg',
      folder: '/home/sonic/videoCatalogue/web/thumbnails'
    });
}, 8);

work.drain = function() {
  console.log('terminata la creazione di tutti i thumbnails');
}
/*
amqp.connect('amqp://localhost:5672', function(err, conn) {
  conn.createChannel(function(err, ch) {
    var q = 'demo';
    ch.assertQueue(q, {durable: true});
    console.log(" [*] Waiting for messages in %s. To exit presss CTRL+C", q);
    ch.consume(q, function(msg) {
      console.log(" [x] Received %s", msg.content.toString('utf8'));
      var content = JSON.parse(msg.content.toString('utf8'));
      work.push(content);
    }, {noAck: true});
  });
});
*/

var connectOptions = {
  'host': 'localhost',
  'port': 61613,
  'connectHeaders': {
    'host': '/',
    'heartbeat': '5000,5000'
  }
};

stompit.connect(connectOptions, function(error, client) {
  if (error) {
    console.log('connect error ' + error.message);
    return;
  }

  var subscribeHeaders = {
   'destination': '/queue/demo',
   'ack': 'client-individual'
  };

  client.subscribe(subscribeHeaders, function(error, message) {
    if (error) {
     console.log('subscribe error ' + error.message);
    }

    message.readString('utf-8', function(error, body) {
      if (error) {
       console.log('error reading message ' + error.message);
      }

      var content = JSON.parse(body);
      var sqlSelect = 'SELECT * FROM video WHERE path = ?';
      var selection = [content.path];
      sqlSelect = mysql.format(sqlSelect, selection);
      console.log(sqlSelect);
      pool.query(sqlSelect, function(error, results, fields) {
         if (results.length <= 0) {
           work.push(content);
           console.log('HIT-->>');
         }
      });
      //work.push(content);
      client.ack(message);
    });
  });
});
