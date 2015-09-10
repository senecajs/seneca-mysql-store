/*jslint node: true */
/*global describe:true, it:true */
/* Copyright (c) 2012 Mircea Alexandru */
/*
 * These tests assume a MySQL database/structure is already created.
 * execute script/schema.sql to create
 */

"use strict";

var _ = require('lodash');
var seneca = require('seneca');
var async = require('async');
var shared = require('seneca-store-test');
var si = seneca();
var extra = require('./mysql.ext.test.js');
var fs = require('fs');

var Lab = require('lab');
var lab = exports.lab = Lab.script();

var describe = lab.describe;
var it = lab.it;

var dbConfig;
if(fs.existsSync(__dirname + '/../test/dbconfig.mine.js')) {
  dbConfig = require('./dbconfig.mine');
} else {
  dbConfig = require('./dbconfig.example');
}

console.log(dbConfig);

var incrementConfig = _.assign(
    {
      map: { '-/-/incremental': '*' },
      auto_increment: true
    }, dbConfig);

si.use(require('..'), dbConfig);

si.use(require('..'), incrementConfig);

si.__testcount = 0;
var testcount = 0;

describe('mysql', function () {
  it('basic', function (done) {
    testcount++;
    shared.basictest(si, done);
  });

  it('sort', function(done){
    testcount++;
    shared.sorttest(si,done);
  });

  it('limits', function(done){
    testcount++;
    shared.limitstest(si,done);
  });

  it('extra', function (done) {
    testcount++;
    extra.test(si, done);
  });

  it('close', function (done) {
    shared.closetest(si, testcount, done);
  });
});

