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
const Q = require('./lib/qbuilder')

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

  async function execQueryAsync (query, db = null) {
    return new Promise((resolve, reject) => {
      const conn = null == db
        ? internals.connectionPool
        : db

      if ('string' === typeof query) {
        return conn.query(query, done)
      }

      return conn.query(query.sql, query.bindings, done)


      function done(err, out) {
        return err ? reject(err) : resolve(out)
      }
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

    //console.dir(query, { depth: 4 }) // dbg

    if (_.isString(query)) {
      db.query(query, done)
    }
    else {
      db.query(query.sql, query.bindings, done)
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

  function generateId (seneca) {
    return new Promise((resolve, reject) => {
      const msg = {
        role: ACTION_ROLE,
        hook: 'generate_id',
        target: STORE_NAME
      }

      return seneca.act(msg, function (err, res) {
        if (err) {
          return reject(err)
        }

        const { id: new_id } = res

        return resolve(new_id)
      })
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


    save: function (args, done) {
      return new Promise(async (resolve, reject) => {
        const seneca = this
        const { ent, q } = args
        const ent_table = RelationalStore.tablename(ent)

        if (isUpdate(ent)) {
          const entp = RelationalStore.makeentp(ent)
          const update_sql = Knex(ent_table).update(entp).where('id', ent.id).toSQL()

          const update = await execQueryAsync(update_sql)
          const updated_anything = update.affectedRows > 0

          if (!updated_anything) {
            const ins_sql = Knex(ent_table).insert(entp).toSQL()

            await execQueryAsync(ins_sql)

            const sel_sql = Knex.select('*').from(ent_table).where('id', ent.id).toSQL()
            const rows = await execQueryAsync(sel_sql)

            if (0 === rows.length) {
              return resolve()
            }

            return resolve(RelationalStore.makeent(ent, rows[0]))
          }

          const sel_sql = Knex.select('*').from(ent_table).where('id', ent.id).toSQL()
          const rows = await execQueryAsync(sel_sql)

          if (0 === rows.length) {
            return resolve()
          }

          return resolve(RelationalStore.makeent(ent, rows[0]))
        }


        const generated_id = await generateId(seneca)

        const new_id = null == ent.id$
          ? generated_id
          : ent.id$


        const new_ent = ent.clone$()
        new_ent.id = new_id

        const new_entp = RelationalStore.makeentp(new_ent)


        const upsert_fields = isUpsert(ent, q)

        if (null != upsert_fields) {
          return transaction(async function (conn, end_of_transaction) {
            try {
              const update_q = upsert_fields
                .filter(c => undefined !== new_entp[c])
                .reduce((h, c) => {
                  h[c] = new_entp[c]
                  return h
                }, {})


              if (_.isEmpty(update_q)) {
                const ins_sql = Knex(ent_table).insert(new_entp).toSQL()

                await execQueryAsync(ins_sql, conn)

                // NOTE: Because MySQL does not support "RETURNING", we must fetch
                // the entity in a separate trip to the db. We can fetch the entity
                // by the query and not worry about duplicates - this is because
                // the query is unique by definition, because upserts can only work
                // for unique keys.
                //
                const sel_sql = Knex.select('*').from(ent_table).where('id', new_ent.id).toSQL()
                const rows = await execQueryAsync(sel_sql, conn)

                if (0 === rows.length) {
                  return end_of_transaction()
                }

                return end_of_transaction(null, RelationalStore.makeent(new_ent, rows[0]))
              }

              // NOTE: This code cannot be replaced with updateEnt. updateEnt updates
              // records by the id. The id in this `ent` is the new id.
              //
              // TODO: Re-consider the logic.
              //
              const update_set = _.clone(new_entp); delete update_set.id
              const update_query = QueryBuilder.updatewherestm(update_q, new_ent, update_set)

              await execQueryAsync({
                sql: update_query.text,
                bindings: update_query.values
              }, conn)

              const ins_select_query = QueryBuilder.insertwherenotexistsstm(new_ent, update_q)

              await execQueryAsync({
                sql: ins_select_query.text,
                bindings: ins_select_query.values
              }, conn)

              // NOTE: Because MySQL does not support "RETURNING", we must fetch
              // the entity in a separate trip to the db. We can fetch the entity
              // by the query and not worry about duplicates - this is because
              // the query is unique by definition, because upserts can only work
              // for unique keys.
              //
              const sel_sql = Knex.select('*').from(ent_table).where(update_q).toSQL()
              const rows = await execQueryAsync(sel_sql, conn)

              if (0 === rows.length) {
                return end_of_transaction()
              }

              return end_of_transaction(null, RelationalStore.makeent(new_ent, rows[0]))
            } catch (err) {
              return end_of_transaction(err)
            }
          }, done)
        }


        const ins_sql = Knex(ent_table).insert(new_entp).toSQL()

        await execQueryAsync(ins_sql)

        const sel_sql = Knex.select('*').from(ent_table).where('id', new_ent.id).toSQL()
        const rows = await execQueryAsync(sel_sql)

        if (0 === rows.length) {
          return resolve()
        }

        return resolve(RelationalStore.makeent(new_ent, rows[0]))
      })
        .then(done).catch(done)

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

    load: function (args, done) {
      return new Promise(async (resolve, reject) => {
        const seneca = this
        const { qent, q } = args

        const sel_sql = Helpers.select(qent, q, seneca, done).toSQL()
        const rows = await execQueryAsync(sel_sql)

        if (0 === rows.length) {
          return resolve(null)
        }

        const out = RelationalStore.makeent(qent, rows[0])

        return resolve(out)
      })
        .then(done).catch(done)
    },


    list: function (args, done) {
      return new Promise(async (resolve, reject) => {
        const seneca = this
        const { qent, q } = args

        const sel_sql = buildListQuery(qent, q)
        const rows = await execQueryAsync(sel_sql)
        const out = rows.map(row => RelationalStore.makeent(qent, row))

        return resolve(out)
      })
        .then(done).catch(done)


      function buildListQuery(qent, q) {
        if ('string' === typeof q.native$) {
          return q.native$
        }

        if (Array.isArray(q.native$)) {
          Assert(0 < q.native$.length, 'q.native$.length')
          const [sql, ...bindings] = q.native$

          return { sql, bindings }
        }

        return Helpers.select(qent, q, seneca).toSQL()
      }
    },

    remove(args, done) {
      const seneca = this
      const { q, qent } = args

      const ent_table = RelationalStore.tablename(qent)

      if (q.all$) {
        const sel_query = Q.selectstm({
          columns: ['id'],
          from: ent_table,
          where: seneca.util.clean(q),
          limit: 0 <= q.limit$ ? q.limit$ : null,
          offset: 0 <= q.skip$ ? q.skip$ : null,
          order_by: q.sort$ || null
        })

        return execQuery(sel_query, function (err, rows) {
          if (err) {
            return done(err)
          }

          const del_query = Q.deletestm({
            from: ent_table,
            where: {
              id: rows.map(x => x.id)
            }
          })

          return execQuery(del_query, function (err, _) {
            if (err) {
              return done(err)
            }

            return done()
          })
        })
      }


      const sel_query = Q.selectstm({
        columns: '*',
        from: ent_table,
        where: seneca.util.clean(q),
        limit: 1,
        offset: 0 <= q.skip$ ? q.skip$ : null,
        order_by: q.sort$ || null
      })


      return execQuery(sel_query, function (err, rows) {
        if (err) {
          return done(err)
        }

        if (0 === rows.length) {
          return done(null, null)
        }


        const row = rows[0]

        const del_query = Q.deletestm({
          from: ent_table,
          where: {
            id: row.id
          }
        })

        return execQuery(del_query, function (err) {
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

class Helpers {
  static select(qent, q, seneca) {
    const ent_table = RelationalStore.tablename(qent)


    let query

    query = Knex.select('*').from(ent_table)


    if ('string' === typeof q) {
      query = query.where({ id: q })
    } else if (Array.isArray(q)) {
      query = query.whereIn('id', q)
    } else {
      const where = seneca.util.clean(q)
      query = query.where(where)
    }


    if ('number' === typeof q.limit$ && 0 <= q.limit$) {
      query = query.limit(q.limit$)
    }


    if ('number' === typeof q.skip$ && 0 <= q.skip$) {
      query = query.offset(q.skip$)
    }


    if (null != q.sort$) {
      const order_by = Object.keys(q.sort$)
        .map(column => {
          const order = q.sort$[column] < 0 ? 'desc' : 'asc'
          return { column, order }
        })

      query = query.orderBy(order_by)
    }

    return query
  }
}

module.exports = mysql_store
