var Transform = require('stream').Transform,
    util = require('util');

var RecordExtractor = function() {
  Transform.call(this, {
    readableObjectMode: true
  });
  this.len = 0;
  this.buf = '';
  this.afterInsert = false;
};

util.inherits(RecordExtractor, Transform);

RecordExtractor.prototype._getNumSequentialChars = function(c,str,idx) {
  var cnt = str[idx] === c ? 1 : 0;
  if (cnt && idx > 0)
    cnt += this._getNumSequentialChars(c,str,idx-1);
  return cnt;
};

RecordExtractor.prototype._getNumQuote = function(part) {
  var numQuote = 0;
  for (var i = 0; i < part.length; i++) {
    if (part[i] === '\'') {
      if (i > 0) {
        var numEsc = this._getNumSequentialChars('\\',part,i-1);
        if (numEsc % 2 === 0)
          numQuote++;
      } else {
        numQuote++;
      }
    }
  }
  return numQuote;
};

RecordExtractor.prototype._parseDataTypes = function(record) {
  for (var i = 0; i < record.length; i++) {
    var item = record[i];

    if (item.indexOf('\'') === 0) {
      item = item.slice(1,item.length-1);
    }

    if (item.length && !isNaN(item)) {
      record[i] = parseFloat(item);
    } else if (item === 'NULL') {
      record[i] = null;
    } else {
      record[i] = item;
    }
  }
};

RecordExtractor.prototype._parseRecord = function(record) {
  var inner = record.slice(1,record.length-1);
  var split = inner.split(',');
  var array = [],
      current = '';
  for (var i = 0; i < split.length; i++) {
    current += split[i];
    var numQuote = this._getNumQuote(current);
    if (numQuote % 2 === 0) {
      array.push(current);
      current = '';
    }
  }
  this._parseDataTypes(array);
  this.push(array);
};

RecordExtractor.prototype._getCloseParen = function(openParen, start) {
  var closeParen = this.buf.indexOf(')', start);
  
  if (closeParen === -1) 
    return closeParen;

  var part = this.buf.slice(openParen,closeParen + 1);
  var numQuote = this._getNumQuote(part);
  if (numQuote % 2 === 1)
    return this._getCloseParen(openParen,closeParen + 1);
  else
    return closeParen;
};

RecordExtractor.prototype._getRecord = function() {
  var openParen = this.buf.indexOf('(');
  var closeParen = this._getCloseParen(openParen, openParen+1);
  if (closeParen === -1) {
    return;
  }
  this._parseRecord(this.buf.slice(openParen, closeParen + 1));
  this.buf = this.buf.slice(closeParen + 1);
  this._getRecord();
};

RecordExtractor.prototype._transform = function(chunk, encoding, callback) {
  var s = chunk.toString();
  if (!this.afterInsert) {
    var insertIndex = s.indexOf('INSERT');
    if (insertIndex !== -1) {
      this.buf += s.slice(insertIndex);
      this.afterInsert = true;
    }
  } else {
    this.buf += s;
  }

  this._getRecord();
  this.len += chunk.length;
  this.emit('progress', this.len);
  callback();
};

module.exports = RecordExtractor;