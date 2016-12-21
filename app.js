

var path = require("path");
var config = require(__dirname + '/config.js');

var mysql = require('mysql');

var connection = mysql.createConnection({
  host: 'localhost',
  user: config.sql.user,
  password: config.sql.password,
  database: config.sql.database
});


var getFiles = function() {
   var q = 'SELECT videos.yid,videos.title,videos.author FROM fights RIGHT JOIN vidstats ON (fights.fid = vidstats.fid) LEFT JOIN videos ON (vidstats.yid = videos.yid)';
   if (config.dev) console.log(q);
   connection.query(q, function(err, rows) {
      if(err) {
         if (config.dev) console.log(err);
      }
      else {
         if(config.dev) console.log(rows);
      }
   });
};








