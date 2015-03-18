var SqlIngester = require('./sqlIngester.js');

var ingester = new SqlIngester({
  fileName: '/run/media/sanders/1TB/wikidump/enwiki-20150304-page.sql',
  queryString: 'INSERT INTO `page` (`page_id`,`page_title`,`page_is_redirect`,`page_len`) VALUES ?',
  insertFunction: function(chunk) {
    return [chunk[0],chunk[2],chunk[5],chunk[10]];
  },
  batchSize: 10000,
  filter: function(chunk) {
    return chunk[1] === 0;
  }
});

ingester.start();