/*
 * Kopiert alle Nodes vom Typ:
 *   - datei
 *   - baujournal
 *   - projektjournal
 * 
 */


var AWS = require('aws-sdk');
var path = require('path');
var fs = require('fs');
var async = require('async');
var mime = require('mime');
var config = require(__dirname + '/config.js');


var mysql = require('mysql');

var connection = mysql.createConnection({
  host: 'localhost',
  user: config.sql.user,
  password: config.sql.password,
  database: config.sql.database
});

var cQuery = function(fields, select, joins, where) {
   for (let i in joins) {
      joins[i] = joins[i][0] + " JOIN " + joins[i][1] + " AS " + joins[i][2] + " ON (" + joins[i][3] + ")";
   }
   var where = where!==undefined&&where instanceof Array&&where.length?" WHERE " + where.join(" AND "):"";
   return "SELECT " + fields.join(",") + " FROM " + select[0] + " AS " + select[1] + " " + joins.join(" ") + where;
};


var getNodes = function(callback) {

   // Liest alle Nodes aus der Tabelle nodes ein
   var gNodes = function(cb) {
      var fields = [
         "n.nid",
         "n.vid",
         "n.title",
         "n.type",
         "cd.field_dokumentendatum_value as datum"
      ];
      var select = ["node", "n"];
      var joins = [
         ["LEFT", "content_type_dossier", "cd", "cd.vid ="+select[1]+".vid"]
      ];
      var where = [
         "n.type IN ('dossier')" // Ausmasskontrolle
      ];
      var q = cQuery(fields, select, joins, where);
      if (config.dev) console.log(q);

      connection.query(q, function(err, nodes) {
         if(err) {
            if (config.dev) console.log(err);
            cb(err);
         }
         else {
            cb(null, nodes);
         }
      });
   };

   // neues Array mit vid als Keys
   var nodesSort = function(nodes, cb) {
      var nodesVid = [];
      for (let i in nodes) {
         nodesVid[nodes[i].vid] = nodes[i];
         nodesVid[nodes[i].vid].files = [];
         nodesVid[nodes[i].vid].terms = {Dateityp:[], Projektgebiet:[], Fachgebiet:[]};
      }
      cb(null, nodesVid);
   };

   // Alle Files auslesen und danach den Nodes zuteilen
   var gFiles = function(nodes, cb) {
      var fields = [
         'cf.vid',
         'f.filename',
         'f.filepath'
      ];
      var select = ["files", "f"];
      var joins = [
         ["LEFT", "content_field_dateien", "cf", "cf.field_dateien_fid="+select[1]+".fid"],
      ];
      var where = [
         "cf.vid IS NOT NULL"
      ];
      var q = cQuery(fields, select, joins, where);
      connection.query(q, function(err, files) {
         if(err) {
            if (config.dev) console.log(err);
            cb(err);
         }
         else {
            for (let i in files) {
               if (files[i].vid in nodes) {
                  nodes[files[i].vid].files.push(files[i]);
               }
            }
            cb(null, nodes);
         }
      });
   };

   // Terms zu nodes hinzufügen
   var gTerms = function(nodes, cb) {
      var fields = [
         'tn.vid',
         'tn.tid',
         'td.name',
         'v.name AS vname',
         'th.parent'
      ];
      var select = ["term_node", "tn"];
      var joins = [
         ["LEFT", "term_data", "td", "td.tid=tn.tid"],
         ["LEFT", "vocabulary", "v", "v.vid=td.vid"],
         ["LEFT", "term_hierarchy", "th", "th.tid=td.tid"]
      ];
      var q = cQuery(fields, select, joins);
      connection.query(q, function(err, terms) {
         if(err) {
            if (config.dev) console.log(err);
            cb(err);
         }
         else {
            for (let i in terms) {
               let vid = terms[i].vid;
               if (vid in nodes) { // wenn vid in node-array als key existiert
                  if (!terms[i].vname in nodes[vid].terms) {
                     nodes[vid].terms[terms[i].vname] = [];
                  }
                  //console.log('vid: ' + vid);
                  //console.log('node: ' + dump(nodes[vid]));
                  //console.log('term: ' + dump(terms[i]));
                  //console.log('Vokabular: ' + terms[i].vname);
                  terms[i].name = terms[i].name.replace(/\//g,', ');
                  nodes[vid].terms[terms[i].vname].push(terms[i]);
               }
            }
            cb(null, nodes);
         }
      });
   };

   // Formatierungen
   var cleanup = function(nodes, cb) {
      for (let i in nodes) {
         nodes[i].title = nodes[i].title.replace(/\<|\>|\?|"|\:|\||\\|\/|\*/g,' ');
         nodes[i].terms['Projektgebiet'] = termOrder(nodes[i].terms['Projektgebiet']);
         nodes[i].datum = cleanupDate([nodes[i].datum]);
      }
      cb(null, nodes)
   };

   // Pfad zu node hinzufügen
   var addPath = function(nodes, cb) {
      for (let i in nodes) {
         nodes[i].path = createPath(nodes[i]);
      };
      cb(null, nodes);
   };

   // Array nach Pfad sortieren
   var sortByPaths = function(nodes, cb) {
      nodes.sort(function(a,b) {
         return a.path.toLowerCase().localeCompare(b.path.toLowerCase());
      });
      cb(null, nodes);
   };

   // Run Waterfall with above functions
   async.waterfall([
      gNodes,
      nodesSort,
      gFiles,
      gTerms,
      cleanup,
      addPath,
      sortByPaths,
   ], function(err, nodes) {
      if (err) callback(err);
      callback(null, nodes);
   });

};


var cleanupDate = function(datum) {
   if (datum[0]===null) {
      if (datum[1]!==null) {
         return datum[1].split('T').shift().replace(/-/g,'');
      }
      else {
         return null;
      }
   }
   return datum[0].split('T').shift().replace(/-/g,'');
};

/*
{
   vid: 5289,
   tid: 8,
   name: 'Abschnitt Tunnel',
   vname: 'Abschnitt',
   parent: 0
}
*/
var termOrder = function (terms) {
   var term1 = 0, term2 = null;
   for (let i in terms) {
      if (terms[i].parent === 0) {
         term1 = terms[i];
      }
      else {
         term2 = terms[i];
      }
   }
   return (term2!==null)?[term1, term2]:[term1];
};

var createPath = function(node) {
   var abschnitt = [];
   for(let i in node.terms.Abschnitt) {
      abschnitt.push(node.terms.Abschnitt[i].name);
   }
   var datum = node.datum===null?'':node.datum+'_';

   var phase = '';
   if (node.type==='baujournal' || node.type==='ausmasskontrolle') {
      phase = 'Realisierung';
   }
   else if (node.type === 'projektjournal') {
      phase = 'Projektierung';
   }
   else {
      phase = node.projektphase;
   }
   // Pfad in der Rheinfolge der einzelnen Elemente
   var pfad = [];
   pfad.push(phase);
   pfad.push(abschnitt.join('/'));
   if (node.type==='baujournal') pfad.push('Baujournal');
   if (node.type==='projektjournal') pfad.push('Projektjournal');
   if (node.terms.Dateityp.length) pfad.push(node.terms.Dateityp[0].name);
   pfad.push(datum + node.title)
   var sPfad = pfad.join('/');
   sPfad = sPfad.replace(/\/\//g, '/');
   sPfad = sPfad.replace(/\s\s\s/g, ' ');
   sPfad = sPfad.replace(/\s\s/g, ' ');
   return sPfad;filf
};


// Execution of script
getNodes(function(err, nodes){
   var count = 0;
   var files = [];
   for (let i in nodes) {
      let s = nodes[i];
      console.log('uploading sample file:');
      console.log(dump(s));
      for (let f in s.files) {
         files.push([s.files[f].filepath, s.path + '/' + s.files[f].filename]);
      }
      count++;
      // For Testing only export first 100 nodes
      if (config.test && count > 100) break;
   }
   async.eachLimit(files, 20, function(f, callback){
      console.log('Copy file: ' + f[0]);
      console.log('to:        ' + f[1]);
      copyFile2S3(f[0], f[1], callback);
   }, function(err) {
      if( err ) {
         console.log('A file failed to process');
      } else {
         console.log('All files have been processed successfully');
      }});
});


var copyFile2S3 = function(localpath, s3path, callback) {
   if (!fs.existsSync(localpath)) {
      fs.appendFile('error.log', 'File does not exist: ' + localpath + "\n", function (err) {
         if (err) throw err;
         callback();
      });
   }
   else {
      var fileBuffer = fs.readFileSync(localpath);
      var contentType = mime.lookup(localpath);
      var s3 = new AWS.S3({
         apiVersion: '2006-03-01',
         accessKeyId: config.s3.key,
         secretAccessKey: config.s3.secret,
         region: config.s3.region,
         s3BucketEndpoint: true,
         endpoint: "http://" + config.s3.bucket + ".s3.amazonaws.com"
      });
      var params = {
         Bucket: config.s3.bucket,
         Key: s3path,
         ACL: 'private',
         Body: fileBuffer,
         ContentType: contentType,
      };
      s3.putObject(params, function(err, data) {
         if(err) console.log(err, err.stack); // an error occurred
         else console.log(data); // successful response
         callback();
      });
   }
};

/**
 * Function : dump()
 * Arguments: The data - array,hash(associative array),object
 *    The level - OPTIONAL
 * Returns  : The textual representation of the array.
 * This function was inspired by the print_r function of PHP.
 * This will accept some data as the argument and return a
 * text that will be a more readable version of the
 * array/hash/object that is given.
 * Docs: http://www.openjs.com/scripts/others/dump_function_php_print_r.php
 */
var dump = function(arr, level) {
   var dumped_text = "";
   if(!level) level = 0;
   //The padding given at the beginning of the line.
   var level_padding = "";
   for(var j = 0; j < level + 1; j++) level_padding += "    ";
   if(typeof(arr) == 'object') { //Array/Hashes/Objects 
      for(var item in arr) {
         var value = arr[item];
         if(typeof(value) == 'object') { //If it is an array,
            dumped_text += level_padding + "'" + item + "' ...\n";
            dumped_text += dump(value, level + 1);
         } else {
            dumped_text += level_padding + "'" + item + "' => \"" + value + "\"\n";
         }
      }
   } else { //Stings/Chars/Numbers etc.
      dumped_text = "===>" + arr + "<===(" + typeof(arr) + ")";
   }
   return dumped_text;
}