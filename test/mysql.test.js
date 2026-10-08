/* jslint node: true */
/* Copyright (c) 2012 Mircea Alexandru */
/*
 * These tests assume a MySQL database/structure is already created.
 * execute script/schema.sql to create
 */

const Seneca = require('seneca')
const Shared = require('seneca-store-test')
const Extra = require('./mysql.ext.test.js')
const Autoincrement = require('./mysql.autoincrement.test.js')

const Lab = require('@hapi/lab')
const lab = exports.lab = Lab.script()
const { describe, before, after } = lab

const DbConfig = require('./support/db/config')


describe('MySQL suite tests ', function () {
  const si = makeSeneca({ mysqlStoreOpts: DbConfig })

  before(() => new Promise((resolve, reject) =>
    si.ready((err) => err ? reject(err) : resolve())))

  after(() => new Promise((resolve, reject) =>
    si.close((err) => err ? reject(err) : resolve())))

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

describe('MySQL autoincrement tests ', function () {
  const incrementConfig = Object.assign(
    {}, DbConfig, {
      map: {'-/-/incremental': '*'},
      auto_increment: true
    }
  )

  const si2 = makeSeneca({ mysqlStoreOpts: incrementConfig })


  before(() => new Promise((resolve, reject) =>
    si2.ready((err) => err ? reject(err) : resolve())))

  after(() => new Promise((resolve, reject) =>
    si2.close((err) => err ? reject(err) : resolve())))

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

