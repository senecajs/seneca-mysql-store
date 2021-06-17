'use strict'

var Assert = require('assert')
var _ = require('lodash')
var MySQL = require('mysql')
var Uuid = require('node-uuid')
var DefaultConfig = require('./default_config.json')
var QueryBuilder = require('./query-builder')
var RelationalStore = require('./lib/relational-util')

var Eraro = require('eraro')({
  package: 'mysql'
})

var storeName = 'mysql-store'
var actionRole = 'sql'

function mysql_store (options) {
  var seneca = this

  var opts = seneca.util.deepextend(DefaultConfig, options)
  var internals = {
    name: storeName,
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

  function execQuery (query, done) {
    if (_.isString(query)) {
      internals.connectionPool.query(query, done)
    }
    else {
      internals.connectionPool.query(query.text, query.values, done)
    }
  }

  function findEnt (ent, q, done) {
    try {
      var query = buildLoadStm(ent, q)

      return execQuery(query, function (err, rows) {
        if (err) {
          return done(err)
        }

        if (rows.length > 0) {
          return done(null, makeEntOfRow(rows[0], ent))
        }

        return done(null, null)
      })
    }
    catch (err) {
      return done(err)
    }
  }

  function listEnts (ent, q, done) {
    try {
      var query = buildListStm(ent, q)

      return execQuery(query, function (err, rows) {
        if (err) {
          return done(err)
        }

        var list = rows.map(function (row) {
          return makeEntOfRow(row, ent)
        })

        return done(null, list)
      })
    }
    catch (err) {
      return done(err)
    }
  }

  function buildLoadStm (ent, q) {
    var loadQ = _.clone(q)
    loadQ.limit$ = 1

    return QueryBuilder.selectstm(ent, loadQ)
  }

  function buildListStm (ent, q) {
    var cq = _.clone(q)
    stripInvalidLimitInPlace(cq)
    stripInvalidSkipInPlace(cq)

    return QueryBuilder.selectstm(ent, cq)
  }

  function stripInvalidLimitInPlace (q) {
    if (Array.isArray(q)) {
      return
    }

    if (!(typeof q.limit$ === 'number' && q.limit$ >= 0)) {
      delete q.limit$
    }
  }

  function stripInvalidSkipInPlace (q) {
    if (Array.isArray(q)) {
      return
    }

    if (!(typeof q.skip$ === 'number' && q.skip$ >= 0)) {
      delete q.skip$
    }
  }

  // TODO: Remove this adapter.
  //
  function makeEntOfRow (row, ent) {
    return RelationalStore.makeent(ent, row)
  }

  // The store interface returned to seneca
  var store = {
    name: storeName,

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


    // Save the data as specified in the entitiy block on the arguments object<br>
    // params<br>
    // <ul>
    // <li>args - of the form { ent: { id: , ..entitiy data..} }<br>
    // <li>done - callback
    // </ul>
    save: function (args, done) {
      Assert(args)
      Assert(done)
      Assert(args.ent)

      var seneca = this
      var autoIncrement = internals.opts.auto_increment || false

      seneca.act({role: actionRole, hook: 'save', target: store.name, auto_increment: autoIncrement}, args, function (err, queryObj) {
        var query = queryObj.query
        var operation = queryObj.operation

        if (err) {
          seneca.log.error('MySQL save error', err)
          return done(err, {code: operation, tag: args.tag$, store: store.name, query: query, error: err})
        }

        execQuery(query, function (err, res) {
          if (err) {
            seneca.log.error(query.text, query.values, err)
            return done(err, {code: operation, tag: args.tag$, store: store.name, query: query, error: err})
          }

          if (!!args.ent && autoIncrement && res.insertId) {
            args.ent.id = res.insertId
          }

          // TODO: Investigate a crash on this line:
          // seneca.log.debug(args.tag$, operation, args.ent)

          return done(null, args.ent)
        })
      })
    },

    load: function (args, done) {
      // var seneca = this

      var qent = args.qent
      var q = args.q

      return findEnt(qent, q, function (err, res) {
        if (err) {
          // TODO: Investigate the crash.
          // seneca.log.error('load', 'Error while fetching the entity:', err)
          return done(err)
        }

        // TODO: Investigate the crash.
        // seneca.log.debug('load', res)

        return done(null, res)
      })
    },


    list: function (args, done) {
      var seneca = this

      var qent = args.qent
      var q = args.q

      return listEnts(qent, q, function (err, res) {
        if (err) {
          seneca.log.error('list', 'Error while listing the entities:', err)
          return done(err)
        }

        seneca.log.debug('list', q, res.length)

        return done(null, res)
      })
    },

    // Delete an item <br>
    // params<br>
    // <ul>
    // <li>args - of the form { ent: { id: , ..entitiy data..} }<br>
    // <li>cb - callback<br>
    // { 'all$': true }
    // </ul>
    remove: function (args, cb) {
      Assert(args)
      Assert(cb)
      Assert(args.qent)
      Assert(args.q)

      var q = args.q

      if (q.load$) {
        store.load(args, function (err, ent) {
          if (err) {
            return cb(err)
          }

          if (!ent) {
            return cb()
          }
          executeRemove(args, ent)
        })
      }
      else {
        executeRemove(args)
      }

      function executeRemove (args, outEnt) {
        var qent = args.qent
        var q = args.q

        var query = QueryBuilder.deletestm(qent, q)

        return execQuery(query, function (err, result) {
          if (err) {
            return cb(err)
          }

          if (q.load$) {
            cb(err, outEnt)
          }
          else {
            cb(err)
          }
        })
      }
    },

    // Return the underlying native connection object
    native: function (args, cb) {
      Assert(args)
      Assert(cb)
      Assert(args.ent)

      cb(null, internals.connectionPool)
    }
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

  seneca.add({role: actionRole, hook: 'load'}, function (args, done) {
    var q = _.clone(args.q)
    var qent = args.qent
    q.limit$ = 1

    QueryBuilder.selectstm(qent, q, function (err, query) {
      return done(err, {query: query})
    })
  })

  seneca.add({role: actionRole, hook: 'save'}, function (args, done) {
    var ent = args.ent
    var update = !!ent.id
    var query
    var autoIncrement = args.auto_increment || false

    if (update) {
      query = QueryBuilder.updatestm(ent)
      return done(null, {query: query, operation: 'update'})
    }

    if (ent.id$) {
      ent.id = ent.id$
      query = QueryBuilder.savestm(ent)
      return done(null, {query: query, operation: 'save'})
    }

    if (autoIncrement) {
      query = QueryBuilder.savestm(ent)
      return done(null, {query: query, operation: 'save'})
    }

    seneca.act({role: actionRole, hook: 'generate_id', target: args.target}, function (err, result) {
      if (err) {
        seneca.log.error('hook generate_id failed')
        return done(err)
      }
      ent.id = result.id
      query = QueryBuilder.savestm(ent)
      return done(null, {query: query, operation: 'save'})
    })
  })

  seneca.add({role: actionRole, hook: 'generate_id', target: store.name}, function (args, done) {
    return done(null, {id: Uuid()})
  })

  return {name: store.name, tag: meta.tag}
}

module.exports = mysql_store
