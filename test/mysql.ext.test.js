/*jslint node: true */
/*jslint asi: true */
/*global describe:true, it:true */
/* Copyright (c) 2010-2015 Richard Rodger */

"use strict";

var assert = require('assert');
var async = require('async');

var scratch = {}
var verify = function(cb,tests){
  return function(error,out) {
    if (error) return cb(error);
    tests(out)
    cb()
  }
}


exports.test = function(si, cb) {
  async.series({
    removeAll: function(cb) {
      console.log('removeAll');
      var foo = si.make({name$:'foo'})
      foo.remove$( {all$:true}, verify(cb, function(err){
      }))
    },
    listEmpty: function(cb) {
      console.log('listEmpty');
      var foo = si.make({name$:'foo'})
      foo.list$({}, verify(cb, function(res){
        assert.equal( 0, res.length)
      }))
    },
    insert2: function(cb) {
      console.log('insert2');
      var foo = si.make({name$:'foo'})
      foo.p1 = 'v1'

      foo.save$(foo, verify(cb, function(foo){
        assert.ok(foo.id)
        assert.equal('v1',foo.p1)
        scratch.foo1 = foo
      }))
    },
    list1: function(cb) {
      console.log('list1');
      scratch.foo1.list$({}, verify(cb, function(res){
        assert.equal( 1, res.length)
      }))
    },

    list2: function(cb) {
      console.log('list2');
      scratch.foo1.list$({id:scratch.foo1.id}, verify(cb, function(res){
        assert.equal( 1, res.length)
      }))
    },
    load1: function(cb) {
      console.log('load1');
      scratch.foo1.load$({id:scratch.foo1.id}, verify(cb, function(res){
        assert.ok(res.id)
      }))
    },

    update: function(cb) {
      console.log('update');
      scratch.foo1.p1 = 'v2'

      scratch.foo1.save$(verify(cb, function(foo){
        assert.ok(foo.id)
        assert.equal('v2',foo.p1)
      }))
    },

    load2: function(cb) {
      console.log('load2');
      scratch.foo1.load$({id:scratch.foo1.id}, verify(cb, function(res){
        assert.equal('v2',res.p1)
      }))
    },

    insertwithsafe: function(cb) {
      console.log('insertwithsafe');
      var foo = si.make({name$:'foo'})
      foo.p1 = 'v3'

      foo.save$(verify(cb, function(foo){
        assert.ok(foo.id)
        assert.equal('v3',foo.p1)
        scratch.foo2 = foo
      }))
    },

    list3: function(cb) {
      console.log('list3');
      scratch.foo2.list$({id:scratch.foo2.id}, verify(cb, function(res){
        assert.equal( 1, res.length)
      }))
    },

    list4: function(cb) {
      console.log('list4');
      scratch.foo2.list$({id:scratch.foo2.id, limit$:1}, verify(cb, function(res){
        assert.equal( 1, res.length)
      }))
    },

    remove1: function(cb) {
      console.log('remove1');
      scratch.foo2.remove$( {id:scratch.foo2.id}, verify(cb, function(err){
      }))
    },

    list5: function(cb) {
      console.log('list5');
      var foo = si.make('foo')
      foo.list$({}, verify(cb, function(res){
        assert.equal( 1, res.length)
      }))
    },

    reportAllErrors: function(cb) {
      console.log('reportAllErrors');
      var foo = si.make('foo')
      foo.missing_attribute = 'v1'

      foo.save$(function (err, foo1) {
        assert.ok(err)
        cb()
      })
    },

    allowAutoIncrementId: function(cb) {
      console.log('allowAutoIncrementId');

      var inc = si.make('incremental')
      inc.p1 = 'v1'

      inc.save$(function (err, inc1) {
        assert.strictEqual(null, err)
        assert.ok(inc1.id)

        inc.load$({id: inc1.id}, verify(cb, function (inc2) {
          assert.strictEqual(null, err)
          assert.ok(inc2)
          assert.equal(inc2.id, inc1.id)
          assert.equal(inc2.p1, 'v1')
        }))
      })
    }
  },
  function(err, out) {
    si.__testcount++;
    cb();
  });
  si.__testcount++;
}

