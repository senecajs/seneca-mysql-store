/*jslint node: true */
/*jslint asi: true */
/*global describe:true, it:true */
/* Copyright (c) 2010-2015 Richard Rodger */

"use strict";

var assert = require('assert');
var async = require('async');
var Lab = require('lab');

var scratch = {}
var verify = function (cb, tests) {
  return function (error, out) {
    if (error) {
      return cb(error);
    }

    try {
      tests(out)
    }
    catch (ex) {
      return cb(ex)
    }

    cb()
  }
}


exports.test = function (settings) {
  var si = settings.seneca
  var must_merge = !!settings.must_merge
  var script = settings.script || Lab.script()

  var describe = script.describe;
  var it = script.it;

  describe('Extra', function () {

    script.before(function (done) {
      var foo = si.make({name$:'foo'})
      foo.remove$( {all$:true}, done)
    })


    it('should report all errors', function reportAllErrors (done) {
      var foo = si.make('foo')
      foo.missing_attribute = 'v1'

      foo.save$(function (err) {
        assert.ok(err)
        done()
      })
    }),

    it('should support auto increment id', function allowAutoIncrementId (done) {

      var inc = si.make('incremental')
      inc.p1 = 'v1'

      inc.save$(function (err, inc1) {
        assert.strictEqual(null, err)
        assert.ok(inc1.id)

        inc.load$({id: inc1.id}, verify(done, function (inc2) {
          assert.ok(inc2)
          assert.equal(inc2.id, inc1.id)
          assert.equal(inc2.p1, 'v1')
        }))
      })

    })
  })

  return script
}

