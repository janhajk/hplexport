

var path = require("path");
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
   return "SELECT " + fields.join(",") + " FROM " + select[0] + " AS " + select[1] + " " + joins.join(" ") + " WHERE " + where.join(" AND ");
};


var getNodes = function(callback) {
   var fields = [
      "n.nid",
      "n.vid",
      "n.title",
      "n.type",
      "cd.field_hplbl_dokumentendatum_value as datum",
      "cd.field_hplbl_description_value as description",
      "pp.field_hplbl_projektphase_value as projektphase",
      "bj.field_baujournal_beschreibung_value as baujournalinhalt",
      "bj.field_baujournaldatum_value as datum_baujournal"
   ];
   var select = ["node", "n"];
   var joins = [
      ["LEFT", "content_type_datei", "cd", "cd.vid ="+select[1]+".vid"],
      ["LEFT", "content_field_hplbl_projektphase", "pp", "pp.vid ="+select[1]+".vid"],
      ["LEFT", "content_type_baujournal", "bj", "bj.vid ="+select[1]+".vid"]
   ];
   var where = [
      "n.type IN ('datei', 'ausmasskontrolle', 'baujournal', 'projektjournal')"
   ];
   var q = cQuery(fields, select, joins, where);
   if (config.dev) console.log(q);
   // q liest nun erst einmal alle nodes in der aktuellsten version

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
            nodesVid[nodes[i].vid].terms = {Abschnitt:[], Dateityp: ''};
         }
         //console.log(nodesVid);
         // Alle Files auslesen und danach den Nodes zuteilen
         var q = "SELECT cf.vid, f.filename, f.filepath FROM files as f LEFT JOIN content_field_hplbl_file as cf ON (cf.field_hplbl_file_fid=f.fid) LEFT JOIN content_field_baujournal_datei as bf ON (bf.field_baujournal_datei_fid=f.fid) WHERE cf.vid IS NOT NULL";
         connection.query(q, function(err, files) {
            if(err) {
               if (config.dev) console.log(err);
               callback(err)
            }
            else {
               //if(config.dev) console.log(files);
               for (let i in files) {
                  if (files[i].vid in nodesVid) {
                     nodesVid[files[i].vid].files.push(files[i]);
                  }
               }
               // Add Terms
               var q = "SELECT tn.vid,tn.tid,td.name,v.name as vname,th.parent FROM term_node as tn LEFT JOIN term_data as td ON (td.tid=tn.tid) LEFT JOIN vocabulary as v ON (v.vid=td.vid) LEFT JOIN term_hierarchy as th ON (th.tid=td.tid)";
               connection.query(q, function(err, terms) {
                  if(err) {
                     if (config.dev) console.log(err);
                     callback(err)
                  }
                  else {
                     //if(config.dev) console.log(terms);
                     for (let i in terms) {
                        if (terms[i].vid in nodesVid) {
                           if (terms[i].vname === 'Dateityp') {
                              nodesVid[terms[i].vid].terms['Dateityp'] = terms[i];
                           }
                           else { // Abschnitt TODO: Hierarchie
                              nodesVid[terms[i].vid].terms['Abschnitt'].push(terms[i]);
                           }
                        }
                     }
                     // cleanup
                     for (let i in nodesVid) {
                        nodesVid[i].title = nodesVid[i].title.replace(/\<|\>|\?|"|\:|\||\\|\/|\*/g,' ');  // /[^a-zA-Z 0-9äöüÄÖÜ\-]+/g
                        nodesVid[i].terms['Abschnitt'] = termOrder(nodesVid[i].terms['Abschnitt']);
                        nodesVid[i].datum = cleanupDate([nodesVid[i].datum, nodesVid[i].datum_baujournal]);
                     }
                     //console.log(dump(nodesVid));
                     callback(null, nodesVid);
                  }
               });
               callback(null, nodesVid);
            }
         });
      }
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
   pfad.push(datum + node.title)
   var sPfad = '/' + pfad.join('/');
   sPfad = sPfad.replace(/\/\//g, '/');
   sPfad = sPfad.replace(/\s\s\s/g, ' ');
   sPfad = sPfad.replace(/\s\s/g, ' ');
   return sPfad;
};

getNodes(function(err, nodes){
   for (let i in nodes) {
      let p = createPath(nodes[i]);
      console.log('nid:' + nodes[i].nid + ': ' + p);
   }
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







