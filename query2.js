

var MongoClient = require('mongodb').MongoClient,
    format = require('util').format,
    assert = require('assert'),
    step = require('step');


var hosts = ['node1.vagrant.dev', 'node2.vagrant.dev', 'node3.vagrant.dev'];
var port = 28000;
//var hosts = ['localhost'];
//var port = 27017;
var db = 'test';
var totalIterations = 1000000;
var batchSize = 100;
var opCount = 0;
var debug = false;

function mConnString (hostList, p) {

    var cStr = 'mongodb://';
    
    for (var i = 0; i < hostList.length; i++) {
        if (i > 0) cStr = cStr + ',';
        cStr = cStr + hostList[i] + ':' + p;
    }

    cStr = cStr  + '/' + db;
    if (debug) console.log("Connection String: ", cStr);
    return cStr;
}

function random (low, high) {

    return Math.floor(Math.random() * (high - low + 1) + low);
};

function getDoc() {
    var doc = {
        "_id" : random(1, 1000),
        "x" : random(1, 10),
        "y" : random(1, 100),
        "z" : random(1, 1000)
    };

    return doc;
};

function updateCol(collection, callback) {
    var doc = getDoc();

    if (debug) console.log("Updating collection with doc: %j", doc);
    
    collection.update({"_id" : doc["_id"]},
                      doc,
                      {upsert: true},
                      function (err, result) {
                          if (err)
                              console.log("Update Error: ", err);
                          else
                              opCount++;
                          callback(err, result);
                      });
};

function readCol(collection, callback) {

    var query = {};
    var querySelect = random(1, 4);


    if (querySelect == 1) {
        query["_id"] = random(1, 1000);
    } else if (querySelect == 2) {
        query["x"] = random(1, 10);
    } else if (querySelect == 3) {
        query["y"] = random(1, 100);
    } else {
        query["z"] = random(1, 1000);
    };

    if (debug) console.log("Reading doc with query %j", query);
    collection.findOne(query, function(error, response) {
        if (error)
            console.log("Read Error: ", error);
        else
            opCount++;
        callback(error, response);
    });
};

function queryBatch(collection, batchSize, batchId, callback) {

    step(
        function processBatch() {
            var group = this.group();
            
            for (var c = 0; c < batchSize; c++) {

                var op = random(1, 5);
        
                if (op == 1) {
                    updateCol(collection, group());
                } else if (op == 2) {
                    updateCol(collection, group());
                } else if (op == 3) {
                    readCol(collection, group());
                } else if (op == 4) {
                    readCol(collection, group());
                } else {
                    readCol(collection, group());
                }
            }
        },
        function done(err, result) {
            if (err)
                callback(err, result);
            else {
                console.log("Batch ", batchId ," complete: ", opCount);
                callback(null, result);
            }
        }
    );
    

}

MongoClient.connect(mConnString(hosts, port), function(err, db) {
    if(err) throw err;

    var collection = db.collection('test');

    for (var i = 0; i < totalIterations; i = i + batchSize) {
        (function (batchId) {
            setImmediate(function () {
                queryBatch(collection, batchSize, batchId, function (err, result) {
                    if (err)
                        console.log("Error in batch processing: ", err);
                });
            });
        })(i);
    }
});





