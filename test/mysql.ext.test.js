/*jslint node: true */
/*jslint asi: true */
/*global describe:true, it:true */
/* Copyright (c) 2010-2012 Richard Rodger */

"use strict";

var seneca = require('seneca')
var chai = require('chai');
chai.Assertion.includeStack = true; 
var assert = chai.assert
var eyes = require('eyes');
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
      var foo = si.make({name$:'foo'})
      foo.remove$( {all$:true}, verify(cb, function(err){
      }))
    },
    listEmpty: function(cb) {
      var foo = si.make({name$:'foo'})
      foo.list$({}, verify(cb, function(res){
        assert.equal( 0, res.length)
      }))
    },
    insert2: function(cb) {
      var foo = si.make({name$:'foo'})
      foo.p1 = 'v1'
      
      foo.save$(foo, verify(cb, function(foo){
        assert.isNotNull(foo.id)
        assert.equal('v1',foo.p1)
        scratch.foo1 = foo
      }))
    },
    list1: function(cb) {
      scratch.foo1.list$({}, verify(cb, function(res){
        assert.equal( 1, res.length)
      }))
    },

    list2: function(cb) {
      scratch.foo1.list$({id:scratch.foo1.id}, verify(cb, function(res){
        assert.equal( 1, res.length)
      }))
    },
    load1: function(cb) {
      scratch.foo1.load$({id:scratch.foo1.id}, verify(cb, function(res){
        assert.isNotNull(res.id)
      }))
    },

    update: function(cb) {
      scratch.foo1.p1 = 'v2'

      scratch.foo1.save$(verify(cb, function(foo){
        assert.isNotNull(foo.id)
        assert.equal('v2',foo.p1)
      }))
    },

    load2: function(cb) {
      scratch.foo1.load$({id:scratch.foo1.id}, verify(cb, function(res){
        assert.equal('v2',res.p1)
      }))
    },

    insertwithsafe: function(cb) {
      var foo = si.make({name$:'foo'})
      foo.p1 = 'v3'
      
      foo.save$(verify(cb, function(foo){
        assert.isNotNull(foo.id)
        assert.equal('v3',foo.p1)
        scratch.foo2 = foo
      }))
    },

    list3: function(cb) {
      scratch.foo2.list$({id:scratch.foo2.id}, verify(cb, function(res){
        assert.equal( 1, res.length)
      }))
    },

    list4: function(cb) {
      scratch.foo2.list$({id:scratch.foo2.id, limit$:1}, verify(cb, function(res){
        assert.equal( 1, res.length)
      }))
    },
    
    // test limit$
    listwithlimit: function(cb) {
      scratch.foo2.list$({limit$:1}, verify(cb, function(res){
        assert.equal( 1, res.length)
      }))
    },
    
    // test sort$
    listwithsort1: function(cb) {
      scratch.foo2.list$({sort$:{'p1':-1}}, verify(cb, function(res){
        assert.equal( 2, res.length)
        assert.equal('v2',res[0].p1)
      }))
    },

    listwithsort2: function(cb) {
      scratch.foo2.list$({sort$:{'p1':1}}, verify(cb, function(res){
        assert.equal( 2, res.length)
        assert.equal('v3',res[0].p1)
      }))
    },
    
    remove1: function(cb) {
      scratch.foo2.remove$( {id:scratch.foo2.id}, verify(cb, function(err){
      }))
    },

    list5: function(cb) {
      var foo = si.make('foo')
      foo.list$({}, verify(cb, function(res){
        assert.equal( 1, res.length)
      }))
    }
  },
  function(err, out) {
    si.__testcount++;
    cb();
  });
  si.__testcount++;
}

