var Cantrip = require("Cantrip");
var mongoInterface = require("../index.js")

describe("This is a test suite for making sure the persistence interface works properly. It tests the getter and setter methods by accessing the database directly", function() {

	it("should set up properly", function(done) {
		Cantrip.options.port = 3002;
		Cantrip.options.namespace = "test" + Math.floor(Math.random() * 10000000000);
		Cantrip.options.persistence = mongoInterface;

		Cantrip.start(function() {
			expect(Cantrip.dataStore).toBeDefined();
			expect(Cantrip.dataStore.data).toBeDefined();
			Cantrip.dataStore.get("/", function(err, res) {
				expect(res).toEqual({});
				done();
			});
		});
	});

	describe("SET methods on root", function() {

		it("key1: simple value", function(done) {
			Cantrip.dataStore.set("/", {key1: "string"}, function(err, res) {
				Cantrip.dataStore.data.find({path: new RegExp("/key1")}, function(err, res) {
					res.toArray(function(err, res) {
						expect(res[0].path).toBe("/key1");
						expect(res[0].value).toBe("string");
						done();
					});
				});
			});
		});

		it("key2: empty array", function(done) {
			Cantrip.dataStore.set("/", {key2: []}, function(err, res) {
				Cantrip.dataStore.data.find({path: new RegExp("/key2")}, function(err, res) {
					res.toArray(function(err, res) {
						expect(res[0].path).toBe("/key2");
						expect(res[0].value).toBe("array");
						done();
					});
				});
			});
		});

		it("key3: empty object", function(done) {
			Cantrip.dataStore.set("/", {key3: {}}, function(err, res) {
				Cantrip.dataStore.data.find({path: new RegExp("/key3")}, function(err, res) {
					res.toArray(function(err, res) {
						
						
						expect(res[0].path).toBe("/key3");
						expect(res[0].value).toBe("object");
						done();
					});
				});
			});
		});

		xit("key4: array with simple values", function(done) {
			Cantrip.dataStore.set("/", {key4: [1,2,3]}, function(err, res) {
				Cantrip.dataStore.data.find({path: new RegExp("/key4")}, function(err, res) {
					res.toArray(function(err, res) {
						console.log(res);
						expect(res.length).toBe(4);
						expect(res[0].path).toBe("/key4");
						expect(res[0].value).toBe("array");

						expect(res[1].path).toBe("/key4/0");
						expect(res[1].value).toBe(1);

						done();
					});
				});
			});
		});

		it("key5: array with objects, accessed through their _id attributes", function(done) {
			Cantrip.dataStore.set("/", {key5: [{_id: "someID1", foo: "bar"}, {_id: "someID2", foo: "bar2"}]}, function(err, res) {
				Cantrip.dataStore.data.find({path: new RegExp("/key5")}, function(err, res) {
					res.toArray(function(err, res) {
						expect(res.length).toBe(7);
						expect(res[0].path).toBe("/key5");
						expect(res[0].value).toBe("array");
						expect(res[1].path).toBe("/key5/someID1");
						expect(res[1].value).toBe("object");
						expect(res[2].path).toBe("/key5/someID1/_id");
						expect(res[2].value).toBe("someID1");
						expect(res[3].path).toBe("/key5/someID1/foo");
						expect(res[3].value).toBe("bar");
						expect(res[4].path).toBe("/key5/someID2");
						expect(res[4].value).toBe("object");
						done();
					});
				});
			});
		});

		it("key6: object with a simple key value pair", function(done) {
			Cantrip.dataStore.set("/", {key6: {foo: "bar"}}, function(err, res) {
				Cantrip.dataStore.data.find({path: new RegExp("/key6")}, function(err, res) {
					res.toArray(function(err, res) {
						expect(res.length).toBe(2);
						expect(res[0].path).toBe("/key6");
						expect(res[0].value).toBe("object");
						expect(res[1].path).toBe("/key6/foo");
						expect(res[1].value).toBe("bar");
						done();
					});
				});
			});
		});

		it("key7: object with a nested object", function(done) {
			Cantrip.dataStore.set("/", {key7: {foo: {bar: "baz"}}}, function(err, res) {
				Cantrip.dataStore.data.find({path: new RegExp("/key7")}, function(err, res) {
					res.toArray(function(err, res) {
						expect(res.length).toBe(3);
						expect(res[0].path).toBe("/key7");
						expect(res[0].value).toBe("object");
						expect(res[1].path).toBe("/key7/foo");
						expect(res[1].value).toBe("object");
						expect(res[2].path).toBe("/key7/foo/bar");
						expect(res[2].value).toBe("baz");
						done();
					});
				});
			});
		});

		it("key8: object with a nested array of objects", function(done) {
			Cantrip.dataStore.set("/", {key8: {foo: [{_id: "nestedID", bar: "baz"}]}}, function(err, res) {
				Cantrip.dataStore.data.find({path: new RegExp("/key8")}, function(err, res) {
					res.toArray(function(err, res) {
						expect(res.length).toBe(5);
						expect(res[0].path).toBe("/key8");
						expect(res[0].value).toBe("object");
						expect(res[1].path).toBe("/key8/foo");
						expect(res[1].value).toBe("array");
						expect(res[2].path).toBe("/key8/foo/nestedID");
						expect(res[2].value).toBe("object");
						expect(res[4].path).toBe("/key8/foo/nestedID/bar");
						expect(res[4].value).toBe("baz");
						done();
					});
				});
			});
		});

	});


	describe("GET methods on ROOT", function() {

		it("key0: accessing a nonexistent route", function(done) {
			Cantrip.dataStore.get("/void", function(err, res) {
				expect(res).toEqual(null);
				//Test nested parameters of a nonexistent key
				Cantrip.dataStore.get("/void/abc", function(err, res) {
					expect(res).toEqual(null);
					done();
				});
			});
		});

		it("key1: accessing a basic value, returns an object with key 'value'", function(done) {
			Cantrip.dataStore.get("/key1", function(err, res) {
				expect(res).toEqual({value: "string"});
				done();
			});
		});

		it("key2: accessing an empty array", function(done) {
			Cantrip.dataStore.get("/key2", function(err, res) {
				expect(res).toEqual([]);
				done();
			});
		});

		it("key3: accessing an empty object", function(done) {
			Cantrip.dataStore.get("/key3", function(err, res) {
				expect(res).toEqual({});
				done();
			});
		});

		it("key4: accessing an array of basic values", function(done) {
			Cantrip.dataStore.get("/key4", function(err, res) {
				expect(res).toEqual([1,2,3]);
				done();
			});
		});

		it("key5: accessing an array of objects", function(done) {
			Cantrip.dataStore.get("/key5", function(err, res) {
				expect(res).toEqual([ 	{ _id: 'someID1', foo: 'bar' },
  										{ _id: 'someID2', foo: 'bar2' } 
  									]);
				done();
			});
		});

		it("key6: accessing an object with a simple key: value pair", function(done) {
			Cantrip.dataStore.get("/key6", function(err, res) {
				expect(res).toEqual({foo: "bar"});
				done();
			});
		});

		it("key7: accessing an object with a nested object", function(done) {
			Cantrip.dataStore.get("/key7", function(err, res) {
				expect(res).toEqual({foo: {bar: "baz"}});
				done();
			});
		});

		it("key8: accessing an object with an array of nested objects", function(done) {
			Cantrip.dataStore.get("/key8", function(err, res) {
				expect(res).toEqual({foo: [{_id: "nestedID", bar: "baz"}]});
				done();
			});
		});

		
	});

	describe("GET methods deeper than root", function() {

		it("key4: accessing a basic element inside an array", function(done) {
			Cantrip.dataStore.get("/key4/0", function(err, res) {
				expect(res).toEqual({value: 1});
				done();
			});
		});

		it("key5: accessing an object inside an array by its _id property", function(done) {
			Cantrip.dataStore.get("/key5/someID1", function(err, res) {
				expect(res).toEqual({_id: "someID1", foo: "bar"});
				done();
			});
		});

		it("key6: accessing a nested value by its key", function(done) {
			Cantrip.dataStore.get("/key6/foo", function(err, res) {
				expect(res).toEqual({value: "bar"});
				done();
			});
		});

		it("key7: accessing a nested bject by its key", function(done) {
			Cantrip.dataStore.get("/key7/foo", function(err, res) {
				expect(res).toEqual({bar: "baz"});
				Cantrip.dataStore.get("/key7/foo/bar", function(err, res) {
					expect(res).toEqual({value: "baz"});
					done();
				});
			});
		});

		it("key8: accessing a nested object by its key", function(done) {
			Cantrip.dataStore.get("/key8/foo", function(err, res) {
				expect(res).toEqual([{_id: "nestedID", bar: "baz"}]);
				Cantrip.dataStore.get("/key8/foo/nestedID", function(err, res) {
					expect(res).toEqual({_id: "nestedID", bar: "baz"});
					Cantrip.dataStore.get("/key8/foo/nestedID/bar", function(err, res) {
						expect(res).toEqual({value: "baz"});
						done();
					});
				});
			});
		});
	});



	describe("SET methods on nested keys", function() {
		it("key1: trying to write a basic value's property should have no effect", function(done) {
			Cantrip.dataStore.set("/key1", {foo: "bar"}, function(err, res) {
				expect(err).toBeDefined();
				expect(res).toEqual(null);
				Cantrip.dataStore.data.find({path: new RegExp("/key1")}, function(err, res) {
					res.toArray(function(err, res) {
						expect(res.length).toBe(1);
						expect(res[0].path).toBe("/key1");
						expect(res[0].value).toBe("string");
						done();
					});
				});
			});
		});

		it("key2: adding an object to an empty array", function(done) {
			Cantrip.dataStore.set("/key2", {foo: "bar", _id: "someID"}, function(err, res) {
				Cantrip.dataStore.data.find({path: new RegExp("/key2")}, function(err, res) {
					res.toArray(function(err, res) {
						expect(res.length).toBe(4);
						expect(res[1].path).toBe("/key2/someID");
						expect(res[1].value).toBe("object");
						expect(res[2].path).toBe("/key2/someID/foo");
						expect(res[2].value).toBe("bar");
						done();
					});
				});
			});
		});

		it("key3: adding a key to an empty object", function(done) {
			Cantrip.dataStore.set("/key3", {foo: "bar"}, function(err, res) {
				Cantrip.dataStore.data.find({path: new RegExp("/key3")}, function(err, res) {
					res.toArray(function(err, res) {
						expect(res.length).toBe(2);
						expect(res[1].path).toBe("/key3/foo");
						expect(res[1].value).toBe("bar");
						done();
					});
				});
			});
		});

		it("key3: adding an object to an object", function(done) {
			Cantrip.dataStore.set("/key3", {obj: {foo: "bar"}}, function(err, res) {
				Cantrip.dataStore.data.find({path: new RegExp("/key3")}, function(err, res) {
					res.toArray(function(err, res) {
						expect(res.length).toBe(4);
						expect(res[2].path).toBe("/key3/obj");
						expect(res[2].value).toBe("object");
						expect(res[3].path).toBe("/key3/obj/foo");
						expect(res[3].value).toBe("bar");
						done();
					});
				});
			});
		});

		it("key5: adding a key to an object inside an array", function(done) {
			Cantrip.dataStore.set("/key5/someID1", {newKey: true}, function(err, res) {
				Cantrip.dataStore.data.find({path: new RegExp("/key5/someID1")}, function(err, res) {
					res.toArray(function(err, res) {
						expect(res.length).toBe(4);
						expect(res[3].path).toBe("/key5/someID1/newKey");
						expect(res[3].value).toBe(true);
						done();
					});
				});
			});
		});
	});

	describe("Overwriting existing values with SET methods", function() {

		it("key1: overwriting a basic value with another basic value on the root", function(done) {
			Cantrip.dataStore.set("/", {key1: "different string"}, function(err, res) {
				Cantrip.dataStore.data.find({path: "/key1"}, function(err, res) {
					res.toArray(function(err, res) {
						expect(res[0].path).toBe("/key1");
						expect(res[0].value).toBe("different string");
						done();
					});
				});
			});
		});

		it("key1: overwriting a basic value with an object on the root", function(done) {
			Cantrip.dataStore.set("/", {key1: {bar: "baz"}}, function(err, res) { //not using foo: bar because that's what we used when we tried to set value on a basic type. If that was successful, the foo key should appear here
				Cantrip.dataStore.data.find({path: new RegExp("/key1")}, function(err, res) {
					res.toArray(function(err, res) {
						expect(res.length).toBe(2);
						expect(res[0].path).toBe("/key1");
						expect(res[0].value).toBe("object");
						expect(res[1].path).toBe("/key1/bar");
						expect(res[1].value).toBe("baz");
						done();
					});
				});
			});
		});

		it("key1: overwriting a basic value on an embedded object", function(done) {
			Cantrip.dataStore.set("/key1", {bar: "newValue"}, function(err, res) {
				Cantrip.dataStore.data.find({path: new RegExp("/key1")}, function(err, res) {
					res.toArray(function(err, res) {
						expect(res[1].value).toBe("newValue");
						done();
					});
				});
			});
		});

		it("key2: replacing an array with contents with a basic value, deleting its contents", function(done) {
			Cantrip.dataStore.set("/", {key2: "foo"}, function(err, res) { 
				Cantrip.dataStore.data.find({path: new RegExp("/key2")}, function(err, res) {
					res.toArray(function(err, res) {
						expect(res.length).toBe(1);
						expect(res[0].path).toBe("/key2");
						expect(res[0].value).toBe("foo");
						done();
					});
				});
			});
		});

		it("key3: replacing an object with contents with a basic value, deleting its contents", function(done) {
			Cantrip.dataStore.set("/", {key3: "foo"}, function(err, res) { 
				Cantrip.dataStore.data.find({path: new RegExp("/key3")}, function(err, res) {
					res.toArray(function(err, res) {
						expect(res.length).toBe(1);
						expect(res[0].path).toBe("/key3");
						expect(res[0].value).toBe("foo");
						done();
					});
				});
			});
		});

		it("key6: replacing the value of a nested object", function(done) {
			Cantrip.dataStore.set("/key6", {foo: "modifiedValue"}, function(err, res) { 
				Cantrip.dataStore.data.find({path: new RegExp("/key6")}, function(err, res) {
					res.toArray(function(err, res) {
						expect(res[1].value).toBe("modifiedValue");
						done();
					});
				});
			});
		});

	});

	describe("DELETING objects", function() {
		it("key1: deleting a key from the root", function(done) {
			Cantrip.dataStore.delete("/key1", function(err, res) {
				Cantrip.dataStore.data.find({path: new RegExp("/key1")}, function(err, res) {
					res.toArray(function(err, res) {
						expect(res.length).toBe(0);
						done();
					});
				});
			});
		});
	});

});

module.exports = Cantrip;