var config = require('./config.json'),
    fs = require('fs'),
    stream = require('stream'),
    RecordExtractor = require('./recordExtractor.js'),
    Promise = require('bluebird'),
    Writable = require('stream').Writable,
    util = require('util');

Promise.promisifyAll(fs);

var fileName = 'files/enwiki-20150304-pagelinks.sql';

var fileSize = 0,
    percentageDone = 0;
var extractor = new RecordExtractor();
extractor.on('progress', function(size) {
  var percentage = Math.floor((size / fileSize)*100);
  if (percentage > percentageDone) {
    percentageDone = percentage;
    console.log('Percentage done: ' + percentageDone);
  }
});

var nameSpaceFilter = new stream.Transform({
  objectMode: true,
  transform: function(chunk, encoding, next) {
    if (chunk[1] === 0 && chunk[3] === 0)
      this.push(chunk);
    next();
  }
});

var recCount = 0;
var counter = new stream.Writable({
  objectMode: true,
  write: function(chunk, encoding, next) {
    recCount++;
    //console.log(chunk);
    //setTimeout(function() { next(); }, 5000);
    next();
  }
});
counter.on('finish', function() {
  console.log(recCount + ' total records.');
})

fs.statAsync(fileName)
  .then(function(res){
    fileSize = res.size;
    fs.createReadStream(fileName)
      .pipe(extractor)
      .pipe(nameSpaceFilter)
      .pipe(counter);
  })
  .catch(function(e) {
    console.log(e);
  });