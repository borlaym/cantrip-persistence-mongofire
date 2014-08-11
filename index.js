var fs = require("fs");
_ = require("lodash");
var mongo = require('mongodb');
var MongoClient = mongo.MongoClient;
// var kue = require("kue");
var Promise = require("node-promise").Promise;
var when = require("node-promise").when;
// var jobs;
var db;

function findInObject(path, object, callback) {
	try {
		for (var i = 0; i < path.split("/").length; i++) {
			object = object[path.split("/")[i]];
		}
		callback(null, object);
	} catch(err) {
		callback(err, null);
	}
};

function mergeCollections(obj, originalPath, collectionNames, callback) {
	var toGet = _.filter(collectionNames, function(coll) {
		return coll.name.split(".")[1].indexOf(originalPath) === 0;
	});
	var promises = [];
	for (var i = 0; i < toGet.length; i++) {
		var path = toGet.name;
		promises.push(function() {
			var promise = new Promise();
			db.collection(path, function(err, coll) {
				coll.find().toArray(function(err, res) {
					path = path.replace(originalPath, "");
					if (path[0] === "/") path = substr(1);
					for (var j = 0; j < path.split("/").length; j++) {
						
					}
					promise.resolve();
				});
			});
			return promise;
		});
	}
	when(promises, function() {
		callback(null, obj);
	});
}

module.exports = {
	setupPersistence: function(callback) {
		var self = this;
		if (!this.options.mongodb) this.options.mongodb = {
			ip: "localhost",
			port: 27017,
			database: "cantrip"
		};
		MongoClient.connect('mongodb://' + this.options.mongodb.ip + ':' + this.options.mongodb.port + '/'+ this.options.mongodb.database, function(err, database) {
			if (err) throw err;
			db = database;
			callback();
		});
	},
	dataStore: {
		get: function(path, callback) {
			var originalPath = path;
			var self = this;
			//Let's see if this collection exists already
			db.collectionNames(function(err, collections) {
				var exists;
				console.log("PATH: ",path);
				exists = _.find(collections, function(coll) {
					return coll.name.split(".")[1] === path;
				});
				while (!exists) {
					path = path.split("/").splice(0, path.split("/").length -1).join("/");
					console.log("PATH: ", path);
					exists = _.find(collections, function(coll) {
						return coll.name.split(".")[1] === path;
					});
				};
				var remainingPath = originalPath.replace(path, "");
				if (remainingPath[0] === "/") remainingPath = remainingPath.substr(1);
				db.collection(path, function(err, coll) {
					var _id = new mongo.ObjectID(remainingPath.split("/")[0]);
					remainingPath = remainingPath.split("/").splice(1).join("/");
					coll.find({_id: _id}).toArray(function(err, res) {
						if (res.length > 0) {
							findInObject(remainingPath, res[0], function(err, obj) {
								mergeCollections(obj, originalPath, collections, callback);
							});
						} else {
							coll.find({"_special" : true}, function(err, res) {
								res = res || [{}];
								findInObject(remainingPath, res[0], function(err, obj) {
									mergeCollections(obj, originalPath, collections, callback);
								});
							});
						}
					});
				});
			});
		},
		set: function(path, data, callback) {
			var self = this;
			db.collection(path, function(err, coll) {
				coll.insert(data, function(err, res) {
					callback(err, res);
				});
			});
		},
		delete: function(path, callback) {
			this.deleteNodes(path, function() {
				callback();
			});
		},
		parent: function(path, callback) {
			var parentPath = path.split("/").slice(0, -1).join("/");
			this.get(parentPath, function(err, res) {
				callback(err, res);
			});
		}
	}
}