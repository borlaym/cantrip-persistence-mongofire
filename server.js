var Cantrip = require("Cantrip");
var mongodb = require("./index.js");
var request = require("request");

Cantrip.options.persistence = mongodb;
Cantrip.options.port = 3000;
Cantrip.options.mongodb = {
	ip: "localhost",
	port: 27017,
	database: "fire"
};

Cantrip.start(function() {
	Cantrip.dataStore.get("/_contents/foo/53e8c7c9cccae64f453a7c0c/a", function(err, res) {
		console.log(res);
	})
});