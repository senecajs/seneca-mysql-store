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
        return do_update(msg, ctx)
      }

      return do_create(msg, ctx)
    }),

    load: asyncmethod(async function (msg) {
      const seneca = this
      const { qent, q } = msg
      const ctx = { seneca, db: internals.connectionPool }


      const ent_table = RelationalStore.tablename(qent)
      const where = where_of_q(q, ctx)


      const out = await selectents({
        ent: qent,
        where,
        limit: 1,
        offset: 0 <= q.skip$ ? q.skip$ : null,
        order_by: q.sort$ || null
      }, ctx)


      if (0 === out.length) {
        return null
      }

      return out[0]
    }),

    list: asyncmethod(async function (msg) {
      const seneca = this
      const ctx = { seneca, db: internals.connectionPool }

      const sel_query = make_query(msg, ctx)
      const rows = await intern.execquery(sel_query, ctx)

      const { qent } = msg

      return rows.map(row => RelationalStore.makeent(qent, row))
    }),

    remove: asyncmethod(async function (msg) {
      const seneca = this
      const { q } = msg
      const ctx = { seneca, db: internals.connectionPool }

      if (q.all$) {
        return remove_all(msg, ctx)
      }

      return remove_one(msg, ctx)
    }),

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


function compact(obj) {
  return Object.keys(obj)
    .map(k => [k, obj[k]])
    .filter(([, v]) => undefined !== v)
    .reduce((acc, [k, v]) => {
      acc[k] = v
      return acc
    }, {})
}


function asyncmethod(f) {
  return function (msg, done) {
    const seneca = this
    const p = f.call(seneca, msg)

    Assert('function' === typeof p.then &&
      'function' === typeof p.catch,
      'The function must be async, i.e. return a promise.')

    return p
      .then(result => done(null, result))
      .catch(done)
  }
}


async function remove_all(msg, ctx) {
  const { seneca } = ctx
  const { q, qent } = msg

  const ent_table = RelationalStore.tablename(qent)

  const rows = await intern.selectrows({
    columns: ['id'],
    from: ent_table,
    where: seneca.util.clean(q),
    limit: 0 <= q.limit$ ? q.limit$ : null,
    offset: 0 <= q.skip$ ? q.skip$ : null,
    order_by: q.sort$ || null
  }, ctx)

  await intern.deleterows({
    from: ent_table,
    where: {
      id: rows.map(x => x.id)
    }
  }, ctx)

  return
}


async function remove_one(msg, ctx) {
  const { seneca } = ctx
  const { q, qent } = msg

  const ent_table = RelationalStore.tablename(qent)

  const out = await selectents({
    ent: qent,
    where: seneca.util.clean(q),
    limit: 1,
    offset: 0 <= q.skip$ ? q.skip$ : null,
    order_by: q.sort$ || null
  }, ctx)


  if (0 === out.length) {
    return null
  }

  const del_ent = out[0]

  await intern.deleterows({
    from: ent_table,
    where: {
      id: del_ent.id
    }
  }, ctx)


  if (q.load$) {
    return del_ent
  }


  return
}


async function insertent (args, ctx) {
  const { ent } = args

  const ent_table = RelationalStore.tablename(ent)
  const entp = RelationalStore.makeentp(ent)

  await intern.insertrow({
    into: ent_table,
    values: compact(entp)
  }, ctx)

  const out = await selectents({
    ent,
    where: { id: ent.id }
  }, ctx)

  if (0 === out.length) {
    return null
  }

  return out[0]
}


async function do_create(msg, ctx) {
  const { ent } = msg
  const { id: gen_id } = await intern.generateid(ctx)

  const new_id = null == ent.id$
    ? gen_id
    : ent.id$

  const new_ent = ent.clone$()
  new_ent.id = new_id


  const upsert_fields = intern.is_upsert(msg)

  if (null != upsert_fields) {
    return upsertent(upsert_fields, { ent: new_ent }, ctx)
  }


  return insertent({ ent: new_ent }, ctx)
}


async function do_update(msg, ctx) {
  const { ent } = msg
  const { id: ent_id } = ent


  const ent_table = RelationalStore.tablename(ent)
  const entp = RelationalStore.makeentp(ent)


  const update = await intern.updaterows({
    table: ent_table,
    set: compact(entp),
    where: { id: ent_id }
  }, ctx)

  const updated_anything = update.affectedRows > 0

  if (!updated_anything) {
    return insertent({ ent }, ctx)
  }


  const out = await selectents({
    ent,
    where: { id: ent.id }
  }, ctx)

  if (0 === out.length) {
    return null
  }

  return out[0]
}


async function upsertent(upsert_fields, args, ctx) {
  const { ent } = args

  const entp = RelationalStore.makeentp(ent)
  const ent_table = RelationalStore.tablename(ent)

  return intern.transaction(async (trx) => {
    const trx_ctx = { ...ctx, db: trx }

    const update_q = upsert_fields
      .filter(c => undefined !== entp[c])
      .reduce((h, c) => {
        h[c] = entp[c]
        return h
      }, {})


    if (_.isEmpty(update_q)) {
      return insertent({ ent }, trx_ctx)
    }

    const update_set = _.clone(entp); delete update_set.id

    await intern.updaterows({
      table: ent_table,
      where: update_q,
      set: update_set
    }, trx_ctx)

    // TODO: TODO:
    //
    const ins_sel_query = QueryBuilder.insertwherenotexistsstm(ent, update_q)
    //
    await intern.execquery({
      sql: ins_sel_query.text,
      bindings: ins_sel_query.values
    }, trx_ctx)

    // NOTE: Because MySQL does not support "RETURNING", we must fetch
    // the entity in a separate trip to the db. We can fetch the entity
    // by the query and not worry about duplicates - this is because
    // the query is unique by definition, because upserts can only work
    // for unique keys.
    //
    const out = await selectents({ ent, where: update_q }, trx_ctx)

    if (0 === out.length) {
      return null
    }

    return out[0]
  }, ctx)
}


function where_of_q(q, ctx) {
  if ('string' === typeof q || Array.isArray(q)) {
    return { id: q }
  }

  const { seneca } = ctx

  return seneca.util.clean(q)
}


function make_query(msg, ctx) {
  const { qent, q } = msg

  if ('string' === typeof q.native$) {
    return q.native$
  }

  if (Array.isArray(q.native$)) {
    Assert(0 < q.native$.length, 'q.native$.length')
    const [sql, ...bindings] = q.native$

    return { sql, bindings }
  }

  const ent_table = RelationalStore.tablename(qent)
  const where = where_of_q(q, ctx)

  return Q.selectstm({
    columns: '*',
    from: ent_table,
    where,
    limit: 0 <= q.limit$ ? q.limit$ : null,
    offset: 0 <= q.skip$ ? q.skip$ : null,
    order_by: q.sort$ || null
  })
}


async function selectents(args, ctx) {
  const { ent } = args
  const from = RelationalStore.tablename(ent)

  const sel_args = { ...args, from, columns: '*' }
  delete sel_args.ent

  const rows = await intern.selectrows(sel_args, ctx)
  const out = rows.map(row => RelationalStore.makeent(ent, row))

  return out
}


module.exports = mysql_store
