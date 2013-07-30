/*jslint node: true */
/*global describe:true, it:true */
/* Copyright (c) 2012 Mircea Alexandru */
/* 
 * These tests assume a MySQL database/structure is already created.
 * execute script/schema.sql to create
 */

"use strict";

var assert = require('assert');
var seneca = require('seneca');
var async = require('async');
var shared = seneca.test.store.shared;
var si = seneca();
var extra = require('./mysql.ext.test.js');

si.use(require('..'), {name:'senecatest',
                       host:'localhost',
                       user:'senecatest',
                       password:'senecatest',
                       port:3306});
si.__testcount = 0;
var testcount = 0;

describe('mysql', function () {
  it('basic', function (done) {
    testcount++;
    shared.basictest(si, done);
  });

  it('extra', function (done) {
    testcount++;
    extra.test(si, done);
  });

  it('close', function (done) {
    shared.closetest(si, testcount, done);
  });
});

