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

  function generateid (ctx, done) {
    const { seneca } = ctx

    const msg = {
      role: ACTION_ROLE,
      hook: 'generate_id',
      target: STORE_NAME
    }

    return seneca.act(msg, function (err, res) {
      if (err) {
        return done(err)
      }

      const { id: new_id } = res

      return done(null, new_id)
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


    save(args, done) {
      const seneca = this
      const { ent, q } = args
      const ent_table = RelationalStore.tablename(ent)

      if (isUpdate(ent)) {
        const entp = RelationalStore.makeentp(ent)

        return intern.updaterows({
          table: ent_table,
          set: compact(entp),
          where: { id: ent.id }
        }, { db: internals.connectionPool }, (err, update) => {
          if (err) {
            return done(err)
          }

          const updated_anything = update.affectedRows > 0

          if (!updated_anything) {
            return intern.insertrow({
              into: ent_table,
              values: compact(entp)
            }, { db: internals.connectionPool }, (err) => {
              if (err) {
                return done(err)
              }

              return intern.selectrows({
                from: ent_table,
                columns: '*',
                where: { id: ent.id }
              }, { db: internals.connectionPool }, (err, rows) => {
                if (err) {
                  return done(err)
                }

                if (0 === rows.length) {
                  return done()
                }

                const row = rows[0]

                return done(null, RelationalStore.makeent(ent, row))
              })
            })
          }

          return intern.selectrows({
            from: ent_table,
            columns: '*',
            where: { id: ent.id }
          }, { db: internals.connectionPool }, (err, rows) => {
            if (err) {
              return done(err)
            }

            if (0 === rows.length) {
              return done()
            }

            const row = rows[0]

            return done(null, RelationalStore.makeent(ent, row))
          })
        })
      }


      return generateid({ seneca }, (err, generated_id) => {
        if (err) {
          return done(err)
        }

        const new_id = null == ent.id$
          ? generated_id
          : ent.id$


        const new_ent = ent.clone$()
        new_ent.id = new_id

        const new_entp = RelationalStore.makeentp(new_ent)


        const upsert_fields = isUpsert(ent, q)

        if (null != upsert_fields) {
          return transaction(async function (trx, end_of_transaction) {
            const update_q = upsert_fields
              .filter(c => undefined !== new_entp[c])
              .reduce((h, c) => {
                h[c] = new_entp[c]
                return h
              }, {})


            if (_.isEmpty(update_q)) {
              return intern.insertrow({
                into: ent_table,
                values: compact(new_entp)
              }, { db: trx }, (err) => {
                if (err) {
                  return end_of_transaction(err)
                }

                // NOTE: Because MySQL does not support "RETURNING", we must fetch
                // the entity in a separate trip to the db. We can fetch the entity
                // by the query and not worry about duplicates - this is because
                // the query is unique by definition, because upserts can only work
                // for unique keys.
                //
                return intern.selectrows({
                  columns: '*',
                  from: ent_table,
                  where: { id: new_ent.id }
                }, { db: trx }, (err, rows) => {
                  if (err) {
                    return end_of_transaction(err)
                  }

                  if (0 === rows.length) {
                    return end_of_transaction()
                  }

                  return end_of_transaction(null, RelationalStore.makeent(new_ent, rows[0]))
                })
              })
            }

            const update_set = _.clone(new_entp); delete update_set.id

            return intern.updaterows({
              table: ent_table,
              where: update_q,
              set: update_set
            }, { db: trx }, (err) => {
              if (err) {
                return end_of_transaction(err)
              }

              // TODO: TODO:
              //
              const ins_select_query = QueryBuilder.insertwherenotexistsstm(new_ent, update_q)

              return intern.execquery(ins_select_query, { db: trx }, (err) => {
                if (err) {
                  return end_of_transaction(err)
                }

                // NOTE: Because MySQL does not support "RETURNING", we must fetch
                // the entity in a separate trip to the db. We can fetch the entity
                // by the query and not worry about duplicates - this is because
                // the query is unique by definition, because upserts can only work
                // for unique keys.
                //
                return intern.selectrows({
                  columns: '*',
                  from: ent_table,
                  where: update_q
                }, { db: internals.connectionPool }, (err, rows) => {
                  if (err) {
                    return end_of_transaction(err)
                  }

                  if (0 === rows.length) {
                    return end_of_transaction()
                  }

                  const row = rows[0]

                  return end_of_transaction(null, RelationalStore.makeent(new_ent, row))
                })
              })
            })
          }, done)
        }


        return intern.insertrow({
          into: ent_table,
          values: compact(new_entp)
        }, { db: internals.connectionPool }, (err) => {
          if (err) {
            return done(err)
          }

          return intern.selectrows({
            columns: '*',
            from: ent_table,
            where: { id: new_ent.id }
          }, { db: internals.connectionPool }, (err, rows) => {
            if (err) {
              return done(err)
            }

            if (0 === rows.length) {
              return done()
            }

            const row = rows[0]

            return done(null, RelationalStore.makeent(new_ent, row))
          })
        })
      })

      function isUpsert (ent, q) {
        if (!Array.isArray(q.upsert$)) {
          return null
        }

        const upsert_fields = q.upsert$.filter((p) => !p.includes('$'))

        if (0 === upsert_fields.length) {
          return null
        }

        return upsert_fields
      }


      function isUpdate (ent) {
        return null != ent.id
      }
    },

    load(args, done) {
      const seneca = this
      const { qent, q } = args
      const ent_table = RelationalStore.tablename(qent)


      let where

      if ('string' === typeof q || Array.isArray(q)) {
        where = { id: q }
      } else {
        where = seneca.util.clean(q)
      }


      return intern.selectrows({
        columns: '*',
        from: ent_table,
        where,
        limit: 1,
        offset: 0 <= q.skip$ ? q.skip$ : null,
        order_by: q.sort$ || null
      }, { db: internals.connectionPool }, (err, rows) => {
        if (err) {
          return done(err)
        }

        if (0 === rows.length) {
          return done(null, null)
        }

        const row = rows[0]
        const out = RelationalStore.makeent(qent, row)

        return done(null, out)
      })
    },


    list(args, done) {
      const seneca = this
      const sel_query = buildListQuery(args, { seneca })

      return intern.execquery(
        sel_query,
        { db: internals.connectionPool },
        (err, rows) => {
          if (err) {
            return done(err)
          }

          const { qent } = args
          const out = rows.map(row => RelationalStore.makeent(qent, row))

          return done(null, out)
        })


      function buildListQuery(args, ctx) {
        const { qent, q } = args
        const { seneca } = ctx

        if ('string' === typeof q.native$) {
          return q.native$
        }

        if (Array.isArray(q.native$)) {
          Assert(0 < q.native$.length, 'q.native$.length')
          const [sql, ...bindings] = q.native$

          return { sql, bindings }
        }

        
        const ent_table = RelationalStore.tablename(qent)


        let where

        if ('string' === typeof q || Array.isArray(q)) {
          where = { id: q }
        } else {
          where = seneca.util.clean(q)
        }


        return Q.selectstm({
          columns: '*',
          from: ent_table,
          where,
          limit: 0 <= q.limit$ ? q.limit$ : null,
          offset: 0 <= q.skip$ ? q.skip$ : null,
          order_by: q.sort$ || null
        })
      }
    },

    remove(args, done) {
      const seneca = this
      const { q, qent } = args

      const ent_table = RelationalStore.tablename(qent)

      if (q.all$) {
        return intern.selectrows({
          columns: ['id'],
          from: ent_table,
          where: seneca.util.clean(q),
          limit: 0 <= q.limit$ ? q.limit$ : null,
          offset: 0 <= q.skip$ ? q.skip$ : null,
          order_by: q.sort$ || null
        }, { db: internals.connectionPool }, (err, rows) => {
          if (err) {
            return done(err)
          }

          return intern.deleterows({
            from: ent_table,
            where: {
              id: rows.map(x => x.id)
            }
          }, { db: internals.connectionPool }, (err, _) => {
            if (err) {
              return done(err)
            }

            return done()
          })
        })
      }


      return intern.selectrows({
        columns: '*',
        from: ent_table,
        where: seneca.util.clean(q),
        limit: 1,
        offset: 0 <= q.skip$ ? q.skip$ : null,
        order_by: q.sort$ || null
      }, { db: internals.connectionPool }, (err, rows) => {
        if (err) {
          return done(err)
        }

        if (0 === rows.length) {
          return done(null, null)
        }


        const row = rows[0]

        return intern.deleterows({
          from: ent_table,
          where: {
            id: row.id
          }
        }, { db: internals.connectionPool }, (err) => {
          if (err) {
            return done(err)
          }

          if (q.load$) {
            return done(null, RelationalStore.makeent(qent, row))
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

  seneca.add({role: ACTION_ROLE, hook: 'load'}, function (args, done) {
    var q = _.clone(args.q)
    var qent = args.qent
    q.limit$ = 1

    QueryBuilder.selectstm(qent, q, function (err, query) {
      return done(err, {query: query})
    })
  })

  seneca.add({role: ACTION_ROLE, hook: 'generate_id', target: store.name}, function (args, done) {
    return done(null, {id: Uuid()})
  })

  return {name: store.name, tag: meta.tag}
}

function compact(obj) {
  return Object.keys(obj)
    .map(k => [k, obj[k]])
    .filter(([, v]) => undefined !== v)
    .reduce((acc, [k, v]) => {
      acc[k] = v
      return acc
    }, {})
}

module.exports = mysql_store
