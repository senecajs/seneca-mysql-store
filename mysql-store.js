const MySQL = require('mysql')
const DefaultConfig = require('./default_config.json')
const Eraro = require('eraro')({ package: 'mysql' })

const Util = require('util')
const { intern } = require('./lib/intern')
const { asyncmethod } = intern

const STORE_NAME = 'mysql-store'

function mysql_store (options) {
  const seneca = this

  const opts = seneca.util.deepextend(DefaultConfig, options)

  const internals = {
    name: STORE_NAME,
    opts
  }

  function configure (spec, done) {
    const conf = get_config(spec)

    const default_conn = {
      connectionLimit: conf.poolSize || 5,
      host: conf.host,
      user: conf.user || conf.username,
      password: conf.password,
      database: conf.name,
      port: conf.port || 3306
    }

    const conn = conf.conn || default_conn

    internals.connectionPool = MySQL.createPool(conn)
    internals.spec = spec

    return internals.connectionPool.getConnection((err, conn) => {
      if (err) {
        seneca.log.error(`Failed to connect to the db as ${conf.username}`, err)
        return done(err)
      }

      seneca.log.debug(`Connected to the db as ${conf.username}`)
      conn.release()

      return done(null, store)
    })

    function get_config(spec) {
      if ('string' === typeof spec) {
        const urlM = /^mysql:\/\/((.*?):(.*?)@)?(.*?)(:?(\d+))?\/(.*?)$/.exec(spec)

        const conf = {
          name: urlM[7],
          server: urlM[4],
          username: urlM[2],
          password: urlM[3],
          port: urlM[6] ? parseInt(conf.port, 10) : null
        }

        return conf
      }

      return spec
    }
  }

  const store = {
    name: STORE_NAME,

    close: asyncmethod(async function (_msg) {
      const { connectionPool: pool = null } = internals

      if (pool) {
        const end = Util.promisify(pool.end).bind(pool)

        try {
          await end()
          seneca.log.debug('Closed the connection to the db')
        } catch (err) {
          seneca.log.error('Failed to close the connection to the db', err)

          throw Eraro({
            code: 'connection/end',
            store: internals.name,
            error: err
          })
        }
      }
    }),

    save: asyncmethod(async function (msg) {
      const seneca = this
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

      const out = await intern.loadent({
        ent: qent,
        where,
        limit: 1,
        offset: 0 <= q.skip$ ? q.skip$ : null,
        order_by: q.sort$ || null
      }, ctx)

      seneca.log.debug('load', 'ok', q, out)

      return out
    }),

    list: asyncmethod(async function (msg) {
      const seneca = this
      const { qent, q } = msg
      const ctx = { seneca, db: internals.connectionPool }


      let out

      const nat_query = intern.is_native(msg)

      if (null == nat_query) {
        const where = intern.where_of_q(q, ctx)

        out = await intern.listents({
          ent: qent,
          where,
          limit: 0 <= q.limit$ ? q.limit$ : null,
          offset: 0 <= q.skip$ ? q.skip$ : null,
          order_by: q.sort$ || null
        }, ctx)
      } else {
        const rows = await intern.execquery(nat_query, ctx)
        out = rows.map(row => intern.makeent(qent, row))
      }

      seneca.log.debug('list', 'ok', q, out.length)

      return out
    }),

    remove: asyncmethod(async function (msg) {
      const seneca = this
      const { q } = msg
      const ctx = { seneca, db: internals.connectionPool }

      let op_name
      let out

      if (q.all$) {
        op_name = 'remove/all'
        out = await intern.remove_many(msg, ctx)
      } else {
        op_name = 'remove/one'
        out = await intern.remove_one(msg, ctx)
      }

      seneca.log.debug(op_name, 'ok', q)

      return out
    }),

    native: asyncmethod(async function (_msg) {
      return internals.connectionPool
    })
  }


  const meta = seneca.store.init(seneca, opts, store)

  internals.desc = meta.desc

  seneca.add({ init: store.name, tag: meta.tag }, function (args, done) {
    configure(internals.opts, function (err) {
      if (err) {
        return done(Eraro(
          'entity/configure',
          'store: ' + store.name,
          'error: ' + err,
          'desc: ' + internals.desc
        ))
      }

      return done()
    })
  })

  return { name: store.name, tag: meta.tag }
}


module.exports = mysql_store
