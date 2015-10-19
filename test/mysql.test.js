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
var shared = require('seneca-store-test');
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


var incrementConfig = _.assign({
  map: { '-/-/incremental': '*' },
  auto_increment: true
}, dbConfig);

var si = seneca();
si.use(require('..'), dbConfig);
si.use(require('..'), incrementConfig);


describe('Mysql', function () {

  shared.basictest({
    seneca: si,
    script: lab
  });

  shared.sorttest({
    seneca: si,
    script: lab
  });

  shared.limitstest({
    seneca: si,
    script: lab
  });

  shared.sqltest({
    seneca: si,
    script: lab
  });

  extra.test({
    seneca: si,
    script: lab
  });

});

