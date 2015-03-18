var SqlIngester = require('./sqlIngester.js');

var ingester = new SqlIngester({
  fileName: '/run/media/sanders/1TB/wikidump/enwiki-20150304-pagelinks.sql',
  queryString: 'INSERT INTO `pagelinks`(`pl_from`,`pl_title`) VALUES ?',
  insertFunction: function(chunk) {
    return [chunk[0],chunk[2]];
  },
  batchSize: 10000,
  filter: function(chunk) {
    return chunk[1] === 0 && chunk[3] === 0;
  }
});

ingester.start();