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
var Extra = require('./mysql.ext.test.js')
var Fs = require('fs')

var Lab = require('lab')
var lab = exports.lab = Lab.script()
var before = lab.before
var describe = lab.describe

var dbConfig
if (Fs.existsSync(__dirname + '/dbconfig.mine.js')) {
  dbConfig = require('./dbconfig.mine')
}
else {
  dbConfig = require('./dbconfig.example')
}

var incrementConfig = {
  map: {'-/-/incremental': '*'},
  auto_increment: true
}

var mysqlConfig = _.assign({}, dbConfig, incrementConfig)

var si = Seneca({
  default_plugins: {
    'mem-store': false
  }
})

describe('MySQL suite tests ', function () {
  before({}, function (done) {
    si.use(require('../mysql-store.js'), dbConfig)
    si.ready(done)
  })

  Shared.basictest({
    seneca: si,
    script: lab
  })

  Shared.sorttest({
    seneca: si,
    script: lab
  })

  Shared.limitstest({
    seneca: si,
    script: lab
  })

  Shared.sqltest({
    seneca: si,
    script: lab
  })

  Extra.extendTest({
    seneca: si,
    script: lab
  })
})
