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
var Autoincrement = require('./mysql.autoincrement.test.js')

var Lab = require('lab')
var lab = exports.lab = Lab.script()
const { describe, before, after } = lab

const DbConfig = require('./support/db/config')


describe('MySQL suite tests ', function () {
  const si = makeSeneca({ mysqlStoreOpts: DbConfig })

  before({}, function (done) {
    si.ready(done)
  })

  after({}, function (done) {
    si.close(done)
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


describe('MySQL autoincrement tests ', function () {
  const incrementConfig = _.assign({}, DbConfig, {
    map: {'-/-/incremental': '*'},
    auto_increment: true
  })

  const si2 = makeSeneca({
    mysqlStoreOpts: incrementConfig
  })


  before({}, function (done) {
    si2.ready(done)
  })

  after({}, function (done) {
    si2.close(done)
  })

  Autoincrement.autoincrementTest({
    seneca: si2,
    script: lab
  })
})


function makeSeneca (opts = {}) {
  const si = Seneca({
    default_plugins: {
      'mem-store': false
    }
  })

  if (si.version >= '2.0.0') {
    si.use('entity')
  }

  const { mysqlStoreOpts = {} } = opts
  si.use(require('../mysql-store.js'), mysqlStoreOpts)

  return si
}

