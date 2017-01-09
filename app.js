

var path = require("path");
var config = require(__dirname + '/config.js');

var mysql = require('mysql');

var connection = mysql.createConnection({
  host: 'localhost',
  user: config.sql.user,
  password: config.sql.password,
  database: config.sql.database
});


var getDateien = function(callback) {
   var fields = [
      "n.nid",
      "n.vid",
      "n.title",
      "cd.field_hplbl_dokumentendatum_value",
      "cd.field_hplbl_description_value",
      "pp.field_hplbl_projektphase_value"
   ];
   var select = ["node", "n"];
   var joins = [
      ["LEFT", "content_type_datei", "cd", "cd.vid ="+select[1]+".vid"],
      ["LEFT", "content_field_hplbl_projektphase", "pp", "pp.vid ="+select[1]+".vid"]
   ];
   var where = [
      "n.type IN ('datei', 'ausmasskontrolle', 'baujournal', 'projektjournal')"
   ];
   var limit = "5";
   // Don't change
   for (let i in joins) {
      joins[i] = joins[i][0] + " JOIN " + joins[i][1] + " AS " + joins[i][2] + " ON (" + joins[i][3] + ")";
   }
   var q = "SELECT " + fields.join(",") + " FROM " + select[0] + " AS " + select[1] + " " + joins.join(" ") + " WHERE " + where.join(" AND ");
   if (config.dev) console.log(q);
   // q liest nun erst einmal alle nodes in der aktuellsten version

   //var q2 = "(SELECT term_node.vid, term_data.name FROM term_node LEFT JOIN term_data ON (term_node.tid = term_data.tid)  WHERE term_data.vid =2)";
   //var q = "SELECT n.*, GROUP_CONCAT(DISTINCT abschnitt.name) as Abschnitte FROM ("+q+") as n RIGHT JOIN " + q2 + " as abschnitt ON (abschnitt.vid = n.vid) WHERE n.nid IS NOT NULL GROUP BY abschnitt.vid LIMIT 0," + limit;

   connection.query(q, function(err, nodes) {
      if(err) {
         if (config.dev) console.log(err);
         callback(err)
      }
      else {
         //if(config.dev) console.log(nodes);
         // neues Array mit vid als Keys
         var nodesVid = {};
         for (let i in nodes) {
            nodesVid[nodes[i].vid] = nodes[i];
            nodesVid[nodes[i].vid].files = [];
         }
         console.log(nodesVid);
         // Alle Files auslesen und danach den Nodes zuteilen
         var q = "SELECT cf.vid, f.filename, f.filepath FROM files as f LEFT JOIN content_field_hplbl_file as cf ON (cf.field_hplbl_file_fid=f.fid)";
         connection.query(q, function(err, files) {
            if(err) {
               if (config.dev) console.log(err);
               callback(err)
            }
            else {
               if(config.dev) console.log(files);
               for (let i in files) {
                  if (files[i].vid in nodesVid) {
                     nodesVid[files[i].vid].files.push(files[i]);
                  }
               }
               console.log(nodesVid);
               callback(null, nodesVid);
            }
         });
      }
   });
};

getDateien(function(err, rows){
   //console.log(rows);
});

var fileExport  = function(filetree) {
   var s3 = require('s3');

   var client = s3.createClient({
      maxAsyncS3: 20,     // this is the default
      s3RetryCount: 3,    // this is the default
      s3RetryDelay: 1000, // this is the default
      multipartUploadThreshold: 20971520, // this is the default (20 MB)
      multipartUploadSize: 15728640, // this is the default (15 MB)
      s3Options: {
         accessKeyId: config.s3.key,
         secretAccessKey: config.s3.secret,
         region: "eu-west-1"
         // See: http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Config.html#constructor-property
      }
   });
   var params = {
      localFile: "some/local/file",

      s3Params: {
         Bucket: config.s3.bucket
      },
   };
   var uploader = client.uploadFile(params);
   uploader.on('error', function(err) {
      console.error("unable to upload:", err.stack);
   });
   uploader.on('progress', function() {
      console.log("progress", uploader.progressMd5Amount,
                  uploader.progressAmount, uploader.progressTotal);
   });
   uploader.on('end', function() {
      console.log("done uploading");
   });
};
/*
getFileTree(function(err, rows){
   export(rows);
});*/







