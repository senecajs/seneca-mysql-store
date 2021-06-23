'use strict'

var Assert = require('assert')
const Async = require('async')
var _ = require('lodash')
var MySQL = require('mysql')
var Uuid = require('node-uuid')
var DefaultConfig = require('./default_config.json')
var QueryBuilder = require('./query-builder')
var RelationalStore = require('./lib/relational-util')
const Knex = require('knex')({ client: 'mysql' })

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

  function execQuery (...args) {
    const [query, db, done] = (function () {
      if (2 === args.length) {
        const [query, done] = args
        return [query, internals.connectionPool, done]
      }

      if (3 === args.length) {
        return args
      }

      throw new Error(`execQuery did not expect ${args.length} args`)
    })()

    if (_.isString(query)) {
      db.query(query, done)
    }
    else {
      db.query(query.text, query.values, done)
    }
  }

  function transaction (f, done) {
    return internals.connectionPool.getConnection(function (err, conn) {
      if (err) {
        return done(err)
      }

      return conn.beginTransaction(function (err) {
        if (err) {
          return conn.rollback(function () {
            conn.release()
            return done(err)
          })
        }

        return f(conn, function (err, out) {
          if (err) {
            return conn.rollback(function () {
              conn.release()
              return done(err)
            })
          }

          return conn.commit(function (err) {
            if (err) {
              return conn.rollback(function () {
                conn.release()
                return done(err)
              })
            }

            conn.release()

            return done(null, out)
          })
        })
      })
    })
  }

  function findEnt (...args) {
    if (3 === args.length) {
      const [ent, q, done] = args

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
    else if (4 === args.length) {
      const [ent, q, conn, done] = args

      try {
        var query = buildLoadStm(ent, q)

        return execQuery(query, conn, function (err, rows) {
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
    else {
      Assert.fail(`Unexpected num of args: ${args.length}`)
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

  function insertEnt (...args) {
    if (2 === args.length) {
      const ent = args[0]
      const done = args[args.length - 1]

      const query = QueryBuilder.savestm(ent)

      return execQuery(query, function (err, res) {
        if (err) {
          return done(err)
        }

        return findEnt(ent, { id: ent.id }, done)
      })
    }
    else if (3 === args.length) {
      const ent = args[0]
      const conn = args[1]
      const done = args[args.length - 1]

      const query = QueryBuilder.savestm(ent)

      return execQuery(query, conn, function (err, res) {
        if (err) {
          return done(err)
        }

        return findEnt(ent, { id: ent.id }, conn, done)
      })
    }
    else {
      Assert.fail(`Unexpected num of args: ${args.length}`)
    }
  }

  function upsertEnt (upsert_fields, ent, done) {
    return transaction(function (conn, end_of_transaction) {
      const update_q = upsert_fields
        .filter(p => null != ent[p])
        .reduce((h, p) => {
          h[p] = ent[p]
          return h
        }, {})

      if (_.isEmpty(update_q)) {
        return insertEnt(ent, conn, end_of_transaction)
      }

      // NOTE: This code cannot be replaced with updateEnt. updateEnt updates
      // records by the id. The id in this `ent` is the new id.
      //
      // TODO: Re-consider the logic.
      //
      const update_set = ent.data$(false); delete update_set.id
      const update_query = QueryBuilder.updatewherestm(update_q, ent, update_set)

      return execQuery(update_query, conn, function (err) {
        if (err) {
          return end_of_transaction(err)
        }

        const ins_select_query = QueryBuilder.insertwherenotexistsstm(ent, update_q)

        //console.dir(ins_select_query, { depth: 32 }) // dbg

        return execQuery(ins_select_query, conn, function (err) {
          if (err) {
            return end_of_transaction(err)
          }

          //console.dir('insert-select ok', { depth: 32 }) // dbg

          // NOTE: Because MySQL does not support "RETURNING", we must fetch
          // the entity in a separate trip to the db. We can fetch the entity
          // by the query and not worry about duplicates - this is because
          // the query is unique by definition, because upserts can only work
          // for unique keys.
          //
          return findEnt(ent, update_q, conn, end_of_transaction)
        })
      })
    }, done)
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
    const query = QueryBuilder.schemastm(ent)

    return execQuery(query, function (err, schema) {
      if (err) {
        return done(err)
      }

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

    if (null != q.native$) {
      return QueryBuilder.nativestm(q)
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


          const upsertFields = isUpsert(ent, q)

          if (null != upsertFields) {
            return upsertEnt(upsertFields, newEnt, function (err, res) {
              if (err) {
                seneca.log.error('save/upsert', 'Error while inserting the entity:', err)
                return done(err)
              }

              seneca.log.debug('save/upsert', res)

              return done(null, res)
            })
          }


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

      function isUpsert (ent, q) {
        if (!Array.isArray(q.upsert$)) {
          return null
        }

        const upsertFields = q.upsert$.filter((p) => !p.includes('$'))

        if (0 === upsertFields.length) {
          return null
        }

        return upsertFields
      }


      function isUpdate (ent) {
        return null != ent.id
      }
    },

    load: function (args, done) {
      const seneca = this


      const { qent } = args
      const ent_table = RelationalStore.tablename(qent)


      const q = _.clone(args.q)

      stripInvalidLimitInPlace(q)
      stripInvalidSkipInPlace(q)


      const where = seneca.util.clean(q)


      let knex_query

      knex_query = Knex.select('*').from(ent_table).where(where).toSQL()

      if (null != q.limit$) {
        knex_query = knex_query.limit(q.limit$)
      }

      if (null != q.skip$) {
        knex_query = knex_query.limit(q.skip$)
      }


      const query = { text: knex_query.sql, values: knex_query.bindings }

      return execQuery(query, function (err, rows) {
        if (err) {
          return done(err)
        }

        if (0 < rows.length) {
          const out = RelationalStore.makeent(qent, rows[0])
          return done(null, out)
        }

        return done(null, null)
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

    // TODO:
    // - Optimize - currently it's SUPER SLOW.
    // - Tidy up
    //
    remove: function (args, done) {
      const { q, qent } = args


      const cq = _.clone(q)

      stripInvalidLimitInPlace(cq)
      stripInvalidSkipInPlace(cq)


      if (cq.all$) {
        return listEnts(qent, cq, function (err, delEnts) {
          if (err) {
            return done(err)
          }

          return Async.parallel(
            delEnts.map(ent => {
              return done => {
                const query = QueryBuilder.deleteentstm(ent)
                return execQuery(query, done)
              }
            }),

            (err) => {
              if (err) {
                return done(err)
              }

              return done()
            }
          )
        })
      }

      return findEnt(qent, cq, function (err, delEnt) {
        if (err) {
          return done(err)
        }

        if (!delEnt) {
          return done()
        }

        const query = QueryBuilder.deleteentstm(delEnt)

        return execQuery(query, (err) => {
          if (err) {
            return done(err)
          }

          if (cq.load$) {
            return done(null, delEnt)
          }

          return done()
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
