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

  function insertEnt (ent, done) {
    var query = QueryBuilder.savestm(ent)

    return execQuery(query, function (err, res) {
      if (err) {
        return done(err)
      }

      return findEnt(ent, { id: ent.id }, done)
    })
  }

  function updateEnt (ent, schema, opts, done) {
    try {
      var merge = shouldMerge(ent, opts)
      var query = QueryBuilder.updatestm(ent, schema, { merge })

      return execQuery(query, done)
    }
    catch (err) {
      return done(err)
    }
  }

  function generateId (seneca, target, done) {
    return seneca.act({ role: actionRole, hook: 'generate_id', target: target }, function (err, res) {
      if (err) {
        return done(err)
      }

      var newId = res.id

      return done(null, newId)
    })
  }

  function shouldMerge (ent, options) {
    if ('merge$' in ent) {
      return Boolean(ent.merge$)
    }

    if (options && ('merge' in options)) {
      return Boolean(options.merge)
    }

    return true
  }

  function getSchema (ent, done) {
    var query = QueryBuilder.schemastm(ent)

    return execQuery(query, function (err, res) {
      if (err) {
        return done(err)
      }

      var schema = res.rows

      return done(null, schema)
    })
  }

  function buildLoadStm (ent, q) {
    var loadQ = _.clone(q)
    loadQ.limit$ = 1

    return QueryBuilder.selectstm(ent, loadQ)
  }

  function buildListStm (ent, q) {
    // TODO: Tidy up.
    //
    if (Array.isArray(q)) {
      return QueryBuilder.selectwhereidinstm(ent, q)
    }

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


    save: function (args, done) {
      var seneca = this

      var ent = args.ent
      var q = args.q

      var autoIncrement = q.auto_increment$ || false

      return getSchema(ent, function (err, schema) {
        if (err) {
          seneca.log.error('save', 'Error while pulling the schema:', err)
          return done(err)
        }


        if (isUpdate(ent)) {
          return updateEnt(ent, schema, opts, function (err, res) {
            if (err) {
              seneca.log.error('save/update', 'Error while updating the entity:', err)
              return done(err)
            }

            var updatedAnything = res.affectedRows > 0

            if (!updatedAnything) {
              return insertEnt(ent, function (err, res) {
                if (err) {
                  seneca.log.error('save/insert', 'Error while inserting the entity:', err)
                  return done(err)
                }

                seneca.log.debug('save/insert', res)

                return done(null, res)
              })
            }

            return findEnt(ent, { id: ent.id }, function (err, res) {
              if (err) {
                seneca.log.error('save/update', 'Error while fetching the updated entity:', err)
                return done(err)
              }

              seneca.log.debug('save/update', res)

              return done(null, res)
            })
          })
        }


        return generateId(seneca, store.name, function (err, generatedId) {
          if (err) {
            seneca.log.error('save/insert', 'Error while generating an id for the entity:', err)
            return done(err)
          }


          var newId = null == ent.id$
            ? generatedId
            : ent.id$


          var newEnt = ent.clone$()

          if (!autoIncrement) {
            newEnt.id = newId
          }


          /* TODO
          if (isUpsert(ent, q)) {
            return upsertEnt(newEnt, q, function (err, res) {
              if (err) {
                seneca.log.error('save/upsert', 'Error while inserting the entity:', err)
                return done(err)
              }

              seneca.log.debug('save/upsert', res)

              return done(null, res)
            })
          }
          */


          return insertEnt(newEnt, function (err, res) {
            if (err) {
              seneca.log.error('save/insert', 'Error while inserting the entity:', err)
              return done(err)
            }

            seneca.log.debug('save/insert', res)

            return done(null, res)
          })
        })
      })

      /* TODO:
      function isUpsert(ent, q) {
        return !isUpdate(ent) &&
          Array.isArray(q.upsert$) &&
          internals.cleanArray(q.upsert$).length > 0
      }
      */


      function isUpdate (ent) {
        return null != ent.id
      }
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

    remove: function (args, done) {
      const { q, qent } = args

      if (q.all$) {
        const query = QueryBuilder.deletestm(qent, q)

        // TODO: seneca.log.debug
        //
        return execQuery(query, function (err) {
          if (err) {
            // TODO: Investigate the crash.
            // seneca.log.error('load', 'Error while fetching the entity:', err)
            return done(err)
          }

          return done()
        })
      }

      return findEnt(qent, q, function (err, delEnt) {
        if (err) {
          // TODO: Investigate the crash.
          // seneca.log.error('load', 'Error while fetching the entity:', err)
          return done(err)
        }

        if (!delEnt) {
          return done()
        }

        const query = QueryBuilder.deleteentstm(delEnt)

        // TODO: seneca.log.debug
        //
        return execQuery(query, function (err) {
          if (err) {
            // TODO: Investigate the crash.
            // seneca.log.error('load', 'Error while fetching the entity:', err)
            return done(err)
          }

          if (q.load$) {
            return done(null, delEnt)
          }

          return done(null, null)
        })
      })
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

  seneca.add({role: actionRole, hook: 'generate_id', target: store.name}, function (args, done) {
    return done(null, {id: Uuid()})
  })

  return {name: store.name, tag: meta.tag}
}

module.exports = mysql_store
