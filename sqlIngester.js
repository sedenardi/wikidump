var config = require('./config.json'),
    fs = require('fs'),
    stream = require('stream'),
    RecordExtractor = require('./recordExtractor.js'),
    Promise = require('bluebird'),
    Writable = require('stream').Writable,
    util = require('util'),
    mysql = require('mysql2');

Promise.promisifyAll(fs);

var SqlIngester = function(options) {
  this.options = options;

  var self = this;
  this.connection;
  this.fileSize = 0;
  this.percentageDone = 0;
  this.recordCount = 0;
  this.records = [];

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
      self.records.push(options.insertFunction(chunk));
      self.recordCount++;
      if (self.records.length === options.batchSize) {
        self.insertRecords(next);
      } else {
        next();
      }    
    }
  });

  this.queryWriter.on('finish', function() {
    self.insertRecords(function() {
      console.log(self.recordCount + ' records inserted. Closing connection.');
      self.connection.end();
    });
  });
};

SqlIngester.prototype.insertRecords = function(next) {
  var self = this;
  var query = this.options.queryFunction(self.records);
  self.connection.query(query.sql, query.inserts, function(err,res) {
    if (err) {
      console.log(err);
      self.connect(function() {
        self.insertRecords(next);
      });
      return;
    }
    if (self.recordCount % 100000 === 0) {
      console.log(self.recordCount + ' records inserted.');
    }
    self.records.length = 0;
    next();
  });
};

SqlIngester.prototype.connect = function(cb) {
  this.connection = mysql.createConnection(config.mysql);
  console.log('MYSQL Connected.');
  cb();
};

SqlIngester.prototype.beginRead = function() {
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

SqlIngester.prototype.start = function() {
  var self = this;
  this.connect(function() {
    self.beginRead();
  });
};

module.exports = SqlIngester;