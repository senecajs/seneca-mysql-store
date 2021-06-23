/* jslint node: true */
/* Copyright (c) 2012 Mircea Alexandru */
/*
 * These tests assume a MySQL database/structure is already created.
 * execute script/schema.sql to create
 */

'use strict'

var Seneca = require('seneca')
var Shared = require('seneca-store-test')
var Extra = require('./mysql.ext.test.js')
var Autoincrement = require('./mysql.autoincrement.test.js')

var Lab = require('@hapi/lab')
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

  Shared.upserttest({
    seneca: si,
    script: lab
  })

  Extra.extendTest({
    seneca: si,
    script: lab
  })
})

describe('', function () {
  const QueryBuilder = require('../query-builder')
  const si = makeSeneca({ mysqlStoreOpts: DbConfig })

/*
  describe('updatewherestm', function () {
    lab.it('', async function () {
      const q = { email: 'richard@voxgig.com', points: 25 }
      const ent = si.make('players')
      const set = { email: 'ceo@voxgig.com', points: 9999 }

      const query = QueryBuilder.updatewherestm(q, ent, set)

      console.dir(query, { depth: 32 }) // dbg
    })
  })
*/

  /*
  describe('insertwherenotexistsstm', function () {
    lab.it('', async function () {
      const ent = si.make('players')
        .data$({ email: 'ceo@voxgig.com', points: 9999 })

      const q = { email: 'ceo@voxgig.com' }

      const query = QueryBuilder.insertwherenotexistsstm(ent, q)

      console.dir(query, { depth: 32 }) // dbg
    })
  })
  */
})

describe('MySQL autoincrement tests ', function () {
  const incrementConfig = Object.assign(
    {}, DbConfig, {
      map: {'-/-/incremental': '*'},
      auto_increment: true
    }
  )

  const si2 = makeSeneca({ mysqlStoreOpts: incrementConfig })


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

