var SqlIngester = require('./sqlIngester.js');

var ingester = new SqlIngester({
  fileName: '/run/media/sanders/1TB/wikidump/enwiki-20150304-redirect.sql',
  insertFunction: function(chunk) {
    return [chunk[0],chunk[2]];
  },
  queryFunction: function(records) {
    return {
      sql: 'INSERT INTO `redirect` (`rd_from`,`rd_title`) VALUES ?',
      inserts: [inserts]
    };
  },
  batchSize: 10000,
  filter: function(chunk) {
    return chunk[1] === 0;
  }
});

ingester.start();