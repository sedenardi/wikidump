var config = require('./config.json'),
    fs = require('fs'),
    stream = require('stream'),
    RecordExtractor = require('./recordExtractor.js'),
    Promise = require('bluebird'),
    Writable = require('stream').Writable,
    util = require('util'),
    mysql = require('mysql');

Promise.promisifyAll(fs);

var SqlIngester = function(options) {
  this.options = options;

  var self = this;
  this.connection = mysql.createConnection(config.mysql);
  this.fileSize = 0;
  this.percentageDone = 0;
  this.recordCount = 0;
  this.inserts = [];

  this.extractor = new RecordExtractor();
  this.extractor.on('progress', function(size) {
    var percentage = Math.floor((size / self.fileSize)*100);
    if (percentage > self.percentageDone) {
      self.percentageDone = percentage;
      console.log('Percentage done: ' + self.percentageDone);
    }
  });

  this.queryWriter = new stream.Writable({
    objectMode: true,
    write: function(chunk, encoding, next) {
      self.inserts.push(options.insertFunction(chunk));
      self.recordCount++;
      if (self.inserts.length === options.batchSize) {
        self.connection.query(options.queryString, [self.inserts], function(err,res) {
          console.log(self.recordCount + ' records inserted.');
          self.inserts.length = 0;
          next();
        });
      } else {
        next();
      }    
    }
  });

  this.queryWriter.on('finish', function() {
    self.connection.query(self.options.queryString, self.inserts, function(err,res) {
      console.log(self.recordCount + ' records inserted. Closing connection.');
      self.connection.end();
    });
  });
};

SqlIngester.prototype.start = function() {
  var self = this;
  fs.statAsync(self.options.fileName)
    .then(function(res){
      self.fileSize = res.size;
      fs.createReadStream(self.options.fileName)
        .pipe(self.extractor)
        .pipe(new stream.Transform({
          objectMode: true,
          transform: function(chunk, encoding, next) {
            if (self.options.filter(chunk))
              this.push(chunk);
            next();
          }
        }))
        .pipe(self.queryWriter);
    })
    .catch(function(e) {
      console.log(e);
    });
};

module.exports = SqlIngester;