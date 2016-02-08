/* jslint node: true */
/* Copyright (c) 2012 Mircea Alexandru */
/*
 * These tests assume a MySQL database/structure is already created.
 * execute script/schema.sql to create
 */

'use strict'

var _ = require('lodash')
var seneca = require('seneca')
var shared = require('seneca-store-test')
var si = seneca()
var extra = require('./mysql.ext.test.js')
var fs = require('fs')

var Lab = require('lab')
var lab = exports.lab = Lab.script()

var describe = lab.describe

var dbConfig
if (fs.existsSync(__dirname + '/dbconfig.mine.js')) {
  dbConfig = require('./dbconfig.mine')
}
else {
  dbConfig = require('./dbconfig.example')
}

console.log(dbConfig)

var incrementConfig = _.assign(
  {
    map: { '-/-/incremental': '*' },
    auto_increment: true
  }, dbConfig)

si.use(require('..'), dbConfig)

si.use(require('..'), incrementConfig)

describe('Level Test', function () {
  shared.basictest({
    seneca: si,
    script: lab
  })

  shared.sorttest({
    seneca: si,
    script: lab
  })

  shared.limitstest({
    seneca: si,
    script: lab
  })

  extra.extendTest({
    seneca: si,
    script: lab
  })
})
