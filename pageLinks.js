var SqlIngester = require('./sqlIngester.js');

var ingester = new SqlIngester({
  fileName: '/run/media/sanders/1TB/wikidump/enwiki-20150304-pagelinks.sql',
  insertFunction: function(chunk) {
    return [chunk[0],chunk[2]];
  },
  queryFunction: function(records) {
    var inserts = [];
    var sql = 'INSERT INTO `links` (`from`,`to`,`redirected`) ';

    var insertSql = 'SELECT ? as `pl_from`, ? as `pl_title`';
    var insertSqls = [];
    records.forEach(function(v,i) {
      inserts.push(v[0]);
      inserts.push(v[1]);
      insertSqls.push(insertSql);
    });

    sql += 'SELECT pl.pl_from as `from`';
    sql += ', coalesce(rp.page_id,p.page_id) as `to`';
    sql += ', case when rp.page_id is not null then 1 else 0 end as `redirected`';
    sql += 'from (' + insertSqls.join(' UNION ALL ') + ') pl ';
    sql += 'inner join page p on p.page_title = pl.pl_title ';
    sql += 'left outer join redirect r on r.rd_from = p.page_id ';
    sql += 'left outer join page rp on rp.page_title = r.rd_title;';

    return {
      sql: sql,
      inserts: inserts
    };
  },
  batchSize: 5000,
  filter: function(chunk) {
    return chunk[1] === 0 && chunk[3] === 0;
  }
});

ingester.start();