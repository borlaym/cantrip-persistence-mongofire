var Cantrip = require("Cantrip");
var mongodb = require("./index.js");
var request = require("request");

Cantrip.options.persistence = mongodb;
Cantrip.options.port = 3000;

Cantrip.start(function() {
	var coll = Cantrip.dataStore.data;
	coll.remove({}, function(err, res) {
		coll.insert([{
			path: "/_contents",
			value: "object"
		}, {
			path: "/_contents/foo",
			value: "array"
		}], function(err, res) {
			for (var i = 0; i < 100; i++) {
				(function(i) {
					request({
						method: "POST",
						url: "http://localhost:3000/foo",
						json: {
							index: i
						}
					}, function(error, response, body) {
						console.log(i);
					});
				})(i);
			}
		});
	})
});