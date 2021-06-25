'use strict'

var Assert = require('assert')
const Async = require('async')
var _ = require('lodash')
var MySQL = require('mysql')
var Uuid = require('node-uuid')
var DefaultConfig = require('./default_config.json')
var QueryBuilder = require('./query-builder')

const RelationalStore = require('./lib/relational-util')
const Q = require('./lib/qbuilder')

const { intern } = require('./lib/intern')
const { asyncmethod } = intern

var Eraro = require('eraro')({
  package: 'mysql'
})

var STORE_NAME = 'mysql-store'
var ACTION_ROLE = 'sql'

function mysql_store (options) {
  var seneca = this

  var opts = seneca.util.deepextend(DefaultConfig, options)
  var internals = {
    name: STORE_NAME,
    opts: opts,
    waitmillis: opts.minwait
  }

  internals.connectionPool = {
    query: function (query, inputs, cb) {
      var startDate = new Date()

      // Print a report abouts operation and time to execute
      function report (err) {
        var log = {
          query: query,
          inputs: inputs,
          time: (new Date()) - startDate
        }

        for (var i in internals.benchmark.rules) {
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
  var reconnect = function () {
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
  function configure (specification, cb) {
    Assert(specification)
    Assert(cb)
    internals.spec = specification

    var conf = 'string' === typeof (internals.spec) ? null : internals.spec
    if (!conf) {
      conf = {}
      var urlM = /^mysql:\/\/((.*?):(.*?)@)?(.*?)(:?(\d+))?\/(.*?)$/.exec(internals.spec)
      conf.name = urlM[7]
      conf.port = urlM[6]
      conf.server = urlM[4]
      conf.username = urlM[2]
      conf.password = urlM[3]
      conf.port = conf.port ? parseInt(conf.port, 10) : null
    }

    var defaultConn = {
      connectionLimit: conf.poolSize || 5,
      host: conf.host,
      user: conf.user || conf.username,
      password: conf.password,
      database: conf.name,
      port: conf.port || 3306
    }
    var conn = conf.conn || defaultConn
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

  // The store interface returned to seneca
  var store = {
    name: STORE_NAME,

    // Close the connection
    close: function (cmd, cb) {
      Assert(cb)

      if (internals.connectionPool) {
        internals.connectionPool.end(function (err) {
          if (err) {
            throw Eraro({code: 'connection/end', store: internals.name, error: err})
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
        return rows.map(row => RelationalStore.makeent(qent, row))
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
  var meta = seneca.store.init(seneca, opts, store)

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

  // TODO: Remove this?
  //
  seneca.add({role: ACTION_ROLE, hook: 'load'}, function (args, done) {
    var q = _.clone(args.q)
    var qent = args.qent
    q.limit$ = 1

    QueryBuilder.selectstm(qent, q, function (err, query) {
      return done(err, {query: query})
    })
  })

  // TODO: Remove this?
  //
  seneca.add({ role: ACTION_ROLE, hook: 'generate_id', target: store.name }, function (args, done) {
    return done(null, intern.generateid())
  })

  return {name: store.name, tag: meta.tag}
}


module.exports = mysql_store
