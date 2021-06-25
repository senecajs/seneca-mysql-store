const Assert = require('assert')
const Async = require('async')
const _ = require('lodash')
const MySQL = require('mysql')
const Uuid = require('node-uuid')
const DefaultConfig = require('./default_config.json')

const Q = require('./lib/qbuilder')

const { intern } = require('./lib/intern')
const { asyncmethod } = intern

const Eraro = require('eraro')({
  package: 'mysql'
})

const STORE_NAME = 'mysql-store'
const ACTION_ROLE = 'sql'

function mysql_store (options) {
  const seneca = this

  const opts = seneca.util.deepextend(DefaultConfig, options)

  const internals = {
    name: STORE_NAME,
    opts: opts,
    waitmillis: opts.minwait
  }

  internals.connectionPool = {
    query: function (query, inputs, cb) {
      const startDate = new Date()

      // Print a report abouts operation and time to execute
      function report (err) {
        const log = {
          query: query,
          inputs: inputs,
          time: (new Date()) - startDate
        }

        for (const i in internals.benchmark.rules) {
          if (log.time > internals.benchmark.rules[i].time) {
            log.tag = internals.benchmark.rules[i].tag
          }
        }

        if (err) {
          log.err = err
        }

        seneca.log[internals.opts.query_log_level || 'debug'](internals.name, log)

        return cb.apply(this, arguments)
      }


      if (cb === undefined) {
        cb = inputs
        inputs = undefined
      }

      if (inputs === undefined) {
        return internals.connectionPool.query(internals.connectionPool, query, report)
      }
      else {
        return internals.connectionPool.query(internals.connectionPool, query, inputs, report)
      }
    },

    escape: function () {
      return internals.connectionPool.escape.apply(internals.connectionPool, arguments)
    },

    format: function () {
      return MySQL.format.apply(MySQL, arguments)
    }
  }

  // Try to reconnect.<br>
  // If error then increase time to wait and try again
  /* TODO: QUESTION: What do we do with this?
   *
  function reconnect() {
    configure(internals.spec, function (err, me) {
      if (err) {
        seneca.log.debug('db reconnect (wait ' + internals.opts.minwait + 'ms) failed: ', err)
        internals.waitmillis = Math.min(2 * internals.waitmillis, internals.opts.maxwait)
        setTimeout(function () {
          reconnect()
        }, internals.waitmillis)
      }
      else {
        internals.waitmillis = internals.opts.minwait
        seneca.log.debug('reconnect ok')
      }
    })
  }
  */


  // Configure the store - create a new store specific connection object<br>
  // Params:<br>
  // <ul>
  // <li>spec - store specific configuration<br>
  // <li>cb - callback
  // </ul>
  function configure (spec, cb) {
    internals.spec = spec

    const conf = 'string' === typeof (internals.spec) ? null : internals.spec
    if (!conf) {
      conf = {}
      const urlM = /^mysql:\/\/((.*?):(.*?)@)?(.*?)(:?(\d+))?\/(.*?)$/.exec(internals.spec)
      conf.name = urlM[7]
      conf.port = urlM[6]
      conf.server = urlM[4]
      conf.username = urlM[2]
      conf.password = urlM[3]
      conf.port = conf.port ? parseInt(conf.port, 10) : null
    }

    const defaultConn = {
      connectionLimit: conf.poolSize || 5,
      host: conf.host,
      user: conf.user || conf.username,
      password: conf.password,
      database: conf.name,
      port: conf.port || 3306
    }
    const conn = conf.conn || defaultConn
    internals.connectionPool = MySQL.createPool(conn)

    // handleDisconnect()
    internals.connectionPool.getConnection(function (err, conn) {
      if (err) {
        return cb(err)
      }

      internals.waitmillis = internals.opts.minwait
      seneca.log.debug({tag$: 'init'}, 'db open and authed for ' + conf.username)
      conn.release()
      cb(null, store)
    })
  }

  const store = {
    name: STORE_NAME,

    close: function (cmd, cb) {
      if (internals.connectionPool) {
        internals.connectionPool.end(function (err) {
          if (err) {
            throw Eraro({ code: 'connection/end', store: internals.name, error: err })
          }
          cb()
        })
      }
      else {
        cb()
      }
    },

    save: asyncmethod(async function (msg) {
      const seneca = this
      const { ent, q } = msg
      const ctx = { seneca, db: internals.connectionPool }

      if (intern.is_update(msg)) {
        return intern.do_update(msg, ctx)
      }

      return intern.do_create(msg, ctx)
    }),

    load: asyncmethod(async function (msg) {
      const seneca = this
      const { qent, q } = msg
      const ctx = { seneca, db: internals.connectionPool }

      const where = intern.where_of_q(q, ctx)

      return intern.loadent({
        ent: qent,
        where,
        limit: 1,
        offset: 0 <= q.skip$ ? q.skip$ : null,
        order_by: q.sort$ || null
      }, ctx)
    }),

    list: asyncmethod(async function (msg) {
      const seneca = this
      const { qent, q } = msg
      const ctx = { seneca, db: internals.connectionPool }

      const nat_query = intern.is_native(msg)

      if (null != nat_query) {
        const rows = await intern.execquery(nat_query, ctx)
        return rows.map(row => intern.makeent(qent, row))
      }

      const where = intern.where_of_q(q, ctx)

      return intern.listents({
        ent: qent,
        where,
        limit: 0 <= q.limit$ ? q.limit$ : null,
        offset: 0 <= q.skip$ ? q.skip$ : null,
        order_by: q.sort$ || null
      }, ctx)
    }),

    remove: asyncmethod(async function (msg) {
      const seneca = this
      const { q } = msg
      const ctx = { seneca, db: internals.connectionPool }

      if (q.all$) {
        return intern.remove_many(msg, ctx)
      }

      return intern.remove_one(msg, ctx)
    }),

    native: asyncmethod(async function (_msg) {
      return internals.connectionPool
    })
  }


  /**
   * Initialization
   */
  const meta = seneca.store.init(seneca, opts, store)

  internals.desc = meta.desc

  seneca.add({init: store.name, tag: meta.tag}, function (args, done) {
    configure(internals.opts, function (err) {
      if (err) {
        seneca.log.error('err: ', err)
        throw Eraro('entity/configure', 'store: ' + store.name, 'error: ' + err, 'desc: ' + internals.desc)
      }
      else done()
    })
  })

  return { name: store.name, tag: meta.tag }
}


module.exports = mysql_store
