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

module.exports = function (options) {
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

        seneca.log(internals.opts.query_log_level, internals.name, log)

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
  var reconnect = function () {
    configure(internals.spec, function (err, me) {
      if (err) {
        seneca.log('db reconnect (wait ' + internals.opts.minwait + 'ms) failed: ', err)
        internals.waitmillis = Math.min(2 * internals.waitmillis, internals.opts.maxwait)
        setTimeout(function () {
          reconnect()
        }, internals.waitmillis)
      }
      else {
        internals.waitmillis = internals.opts.minwait
        seneca.log('reconnect ok')
      }
    })
  }


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
      seneca.log({tag$: 'init'}, 'db open and authed for ' + conf.username)
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

          seneca.log(args.tag$, operation, args.ent)
          return done(null, args.ent)
        })
      })
    },

    // Load first matching item based on id<br>
    // params<br>
    // <ul>
    // <li>args - of the form { ent: { id: , ..entitiy data..} }<br>
    // <li>done - callback<br>
    // </ul>
    load: function (args, done) {
      Assert(args)
      Assert(done)
      Assert(args.qent)
      Assert(args.q)

      var q = _.clone(args.q)
      var qent = args.qent
      q.limit$ = 1

      seneca.act({role: actionRole, hook: 'load', target: store.name}, args, function (err, queryObj) {
        var query = queryObj.query

        if (err) {
          seneca.log.error(query, err)
          return done(err, {code: 'load', tag: args.tag$, store: store.name, query: query, error: err})
        }

        execQuery(query, function (err, res, fields) {
          if (err) {
            seneca.log(args.tag$, 'load', err)
            return done(err)
          }

          var ent = RelationalStore.makeent(qent, res[0])

          seneca.log(args.tag$, 'load', ent)

          done(null, ent)
        })
      })
    },


    // Return a list of object based on the supplied query, if no query is supplied
    // then 'select * from ...'<br>
    // Notes: trivial implementation and unlikely to perform well due to list copy
    //        also only takes the first page of results from simple DB should in fact
    //        follow paging model<br>
    // params:<br>
    // <ul>
    // <li>args - of the form { ent: { id: , ..entitiy data..} }<br>
    // <li>cb - callback<br>
    // a=1, b=2 simple<br>
    // next paging is optional in simpledb<br>
    // limit$ -><br>
    // use native$<br>
    // </ul>
    list: function (args, done) {
      Assert(args)
      Assert(done)
      Assert(args.qent)
      Assert(args.q)

      var qent = args.qent

      seneca.act({role: actionRole, hook: 'list', target: store.name}, args, function (err, queryObj) {
        var query = queryObj.query

        if (err) {
          seneca.log.error(query, err)
          return done(err, {code: 'list', tag: args.tag$, store: store.name, query: query, error: err})
        }

        execQuery(query, function (err, results) {
          if (err) {
            return done(err)
          }

          var list = []
          results.forEach(function (row) {
            var ent = RelationalStore.makeent(qent, row)
            list.push(ent)
          })
          done(null, list)
        })
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
        store.load(args, function (err, row) {
          if (err) {
            return cb(err)
          }

          if (!row) {
            return cb()
          }
          executeRemove(args, row)
        })
      }
      else {
        executeRemove(args)
      }

      function executeRemove (args, row) {
        seneca.act({role: actionRole, hook: 'remove', target: store.name}, args, function (err, queryObj) {
          var query = queryObj.query
          if (err) {
            return cb(err)
          }
          execQuery(query, function (err, result) {
            if (err) {
              return cb(err)
            }

            if (q.load$) {
              cb(err, row)
            }
            else {
              cb(err)
            }
          })
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
        console.log('err: ', err)
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

  seneca.add({role: actionRole, hook: 'list'}, function (args, done) {
    var qent = args.qent
    var q = args.q

    buildSelectStatement(q, function (err, query) {
      return done(err, {query: query})
    })

    function buildSelectStatement (q, done) {
      var query

      if (_.isString(q)) {
        return done(null, q)
      }
      else if (_.isArray(q)) {
        // first element in array should be query, the other being values
        if (q.length === 0) {
          var errorDetails = {
            message: 'Invalid query',
            query: q
          }
          seneca.log.error('Invalid query')
          return done(errorDetails)
        }
        query = {}
        // query.text = QueryBuilder.fixPrepStatement(q[0])
        query.text = q[0]
        query.values = _.clone(q)
        query.values.splice(0, 1)
        return done(null, query)
      }
      else {
        if (q.ids) {
          return done(null, QueryBuilder.selectstmOr(qent, q))
        }
        else {
          QueryBuilder.selectstm(qent, q, done)
        }
      }
    }
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

  seneca.add({role: actionRole, hook: 'remove'}, function (args, done) {
    var qent = args.qent
    var q = args.q

    var query = QueryBuilder.deletestm(qent, q)
    return done(null, {query: query})
  })

  seneca.add({role: actionRole, hook: 'generate_id', target: store.name}, function (args, done) {
    return done(null, {id: Uuid()})
  })

  return {name: store.name, tag: meta.tag}
}
