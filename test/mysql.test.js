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
  class Q {
    static insertstm(args) {
      const { table, values } = args

      const col_names = Object.keys(values)
      const col_vals = Object.values(values)


      let bindings = []
      let sql = ''


      sql += 'insert into ?? '
      bindings.push(table)


      const col_placeholders = col_names.map(_ => '??')
      sql += '(' + col_placeholders.join(', ') + ') '
      bindings = bindings.concat(col_names)


      const val_placeholders = col_vals.map(_ => '?')
      sql += 'values (' + val_placeholders.join(', ') + ') '
      bindings = bindings.concat(col_vals)


      return { sql, bindings }
    }

    static wherestm(args) {
      const { where } = args
      const update_all = 0 === Object.keys(where).length


      let sql = ''
      let bindings = []


      if (update_all) {
        sql += '1'
      } else {
        let first_where = true

        for (const where_col in where) {
          const where_val = where[where_col]

          if (!first_where) {
            sql += ', '
          }

          if (null == where_val) {
            sql += '?? is null'
          } else {
            sql += '?? = ?'
          }

          bindings.push(where_col)
          bindings.push(where_val)

          first_where = false
        }
      }

      return { sql, bindings }
    }

    static updatestm(args) {
      const { table, set, where } = args


      let bindings = []
      let sql = ''


      sql += 'update ?? '
      bindings.push(table)

      sql += 'set '


      let first_set = true

      for (const set_col in set) {
        const set_val = set[set_col]

        if (!first_set) {
          sql += ', '
        }

        sql += '?? = ?'
        bindings.push(set_col)
        bindings.push(set_val)

        first_set = false
      }

      sql += ' '


      const where_q = Q.wherestm({ where })

      sql += 'where ' + where_q.sql
      bindings = bindings.concat(where_q.bindings)


      return { sql, bindings }
    }

    static selectstm(args) {
      const {
        table,
        columns = '*',
        where = null,
        offset = null,
        limit = null,
        order_by = null
      } = args


      let bindings = []
      let sql = ''


      sql += 'select '


      if ('*' === columns) {
        sql += '*'
      } else {
        const col_placeholders = columns.map(_ => '??')
        sql += col_placeholders.join(', ')

        bindings = bindings.concat(columns)
      }


      sql += ' from ??'
      bindings.push(table)


      if (null != where) {
        const where_q = Q.wherestm({ where })

        sql += ' where ' + where_q.sql
        bindings = bindings.concat(where_q.bindings)
      }


      if (null != order_by) {
        sql += ' order by '


        let first_pair = true

        for (const order_col in order_by) {
          const order_val = order_by[order_col]


          let order
          
          if ('string' === typeof order_val) {
            if ('desc' === order_val.toLowerCase()) {
              order = 'desc'
            } else if ('asc' === order_val.toLowerCase()) {
              order = 'asc'
            } else {
              throw new Error(`Unknown order: ${order_val}`)
            }
          } else if ('number' === typeof order_val) {
            order = 0 <= order_val ? 'asc' : 'desc'
          } else {
            throw new Error('order must be a number or a string')
          }

          if (!first_pair) {
            sql += ', '
          }

          sql += '?? ' + order
          bindings.push(order_col)

          first_pair = false
        }
      }


      if (null != limit) {
        sql += ' limit ?'
        bindings.push(limit)
      }


      if (null != offset) {
        sql += ' offset ?'
        bindings.push(offset)
      }


      return { sql, bindings }
    }
  }

  describe('updatestm', () => {
    lab.it('selectstm', async () => {
      const selectstm = Q.selectstm({
        table: 'users',
        columns: ['id', 'email'],
        offset: 0,
        order_by: { email: 1, id: 'asc', age: 'desc', score: -1 }
      })

      console.dir(selectstm, { depth: 32 }) // dbg
    })

    lab.it('', async () => {
      const updatestm = Q.updatestm({
        table: 'users',
        set: {
          id: 'aaa',
          email: 'rr@voxgig.com',
          meta: JSON.stringify({ foo: 'bar' })
        },
        where: { id: 'aaa' }
      })

      console.dir(updatestm, { depth: 32 }) // dbg
    })
  })

  describe('insertstm', () => {
    lab.it('', async () => {
      const insstm = Q.insertstm({
        table: 'users',
        values: {
          id: 'aaa',
          email: 'rr@voxgig.com',
          meta: JSON.stringify({ foo: 'bar' })
        }
      })

      console.dir(insstm, { depth: 32 }) // dbg
    })
  })

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

