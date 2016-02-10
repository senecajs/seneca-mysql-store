/* jslint node: true */
/* Copyright (c) 2012 Mircea Alexandru */
/*
 * These tests assume a MySQL database/structure is already created.
 * execute script/schema.sql to create
 */

'use strict'

var _ = require('lodash')
var Seneca = require('seneca')
var Shared = require('seneca-store-test')
var Si = Seneca()
var Extra = require('./mysql.ext.test.js')
var Fs = require('fs')

var Lab = require('lab')
var lab = exports.lab = Lab.script()

var describe = lab.describe

var dbConfig
if (Fs.existsSync(__dirname + '/dbconfig.mine.js')) {
>>>>>>> cde3f963f26b964be53d084ff81b8c3c32b1e6f1
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

Si.use(require('..'), dbConfig)

Si.use(require('..'), incrementConfig)

describe('Level Test', function () {
  Shared.basictest({
    seneca: Si,
    script: lab
  })

  Shared.sorttest({
    seneca: Si,
    script: lab
  })

  Shared.limitstest({
    seneca: Si,
    script: lab
  })

  Extra.extendTest({
    seneca: Si,
    script: lab
  })
})
