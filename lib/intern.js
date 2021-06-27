const Assert = require('assert')
const Util = require('util')
const Uuid = require('uuid')
const Q = require('./qbuilder')


const intern = {
  generateid() {
    return { id: Uuid() }
  },


  async insertrow(args, ctx) {
    const query = Q.insertstm(args)
    return intern.execquery(query, ctx)
  },


  async insertrowwherenotexists(args, ctx) {
    const query = Q.insertwherenotexistsstm(args)
    return intern.execquery(query, ctx)
  },


  async updaterows(args, ctx) {
    const query = Q.updatestm(args)
    return intern.execquery(query, ctx)
  },


  async deleterows(args, ctx) {
    const query = Q.deletestm(args)
    return intern.execquery(query, ctx)
  },


  async selectrows(args, ctx) {
    const query = Q.selectstm(args)
    return intern.execquery(query, ctx)
  },


  async execquery(query, ctx) {
    const { db } = ctx
    const exec = Util.promisify(db.query).bind(db)

    if ('string' === typeof query) {
      return exec(query)
    }

    return exec(query.sql, query.bindings)
  },


  is_upsert(msg) {
    const { q } = msg

    if (!Array.isArray(q.upsert$)) {
      return null
    }

    const upsert_fields = q.upsert$.filter((p) => !p.includes('$'))

    if (0 === upsert_fields.length) {
      return null
    }

    return upsert_fields
  },


  is_update(msg) {
    const { ent } = msg
    return null != ent.id
  },


  async transaction(f, ctx) {
    const { db } = ctx

    const getConnection = Util.promisify(db.getConnection).bind(db)
    const trx = await getConnection()

    try {
      const beginTransaction = Util.promisify(trx.beginTransaction).bind(trx)
      const commit = Util.promisify(trx.commit).bind(trx)
      const rollback = Util.promisify(trx.rollback).bind(trx)

      try {
        await beginTransaction()
        const result = await f(trx)
        await commit()

        return result
      } catch (err) {
        await rollback()
        throw err
      }
    } finally {
      trx.release()
    }
  },


  compact(obj) {
    return Object.keys(obj)
      .map(k => [k, obj[k]])
      .filter(([, v]) => undefined !== v)
      .reduce((acc, [k, v]) => {
        acc[k] = v
        return acc
      }, {})
  },


  asyncmethod(f) {
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
  },


  async remove_many(msg, ctx) {
    const { seneca } = ctx
    const { q, qent } = msg


    const ent_table = intern.tablename(qent)

    const rows = await intern.selectrows({
      columns: ['id'],
      from: ent_table,
      where: seneca.util.clean(q),
      limit: 0 <= q.limit$ ? q.limit$ : null,
      offset: 0 <= q.skip$ ? q.skip$ : null,
      order_by: q.sort$ || null
    }, ctx)


    await intern.deleteent({
      ent: qent,
      where: {
        id: rows.map(x => x.id)
      }
    }, ctx)


    return
  },


  async remove_one(msg, ctx) {
    const { seneca } = ctx
    const { q, qent } = msg


    const del_ent = await intern.loadent({
      ent: qent,
      where: seneca.util.clean(q),
      offset: 0 <= q.skip$ ? q.skip$ : null,
      order_by: q.sort$ || null
    }, ctx)


    if (null === del_ent) {
      return null
    }


    await intern.deleteent({
      ent: qent,
      where: {
        id: del_ent.id
      }
    }, ctx)


    if (q.load$) {
      return del_ent
    }


    return null
  },


  async insertent (args, ctx) {
    const { ent } = args

    const ent_table = intern.tablename(ent)
    const entp = intern.makeentp(ent)

    await intern.insertrow({
      into: ent_table,
      values: intern.compact(entp)
    }, ctx)

    return intern.loadent({
      ent,
      where: { id: ent.id }
    }, ctx)
  },


  make_insertable(ent, ctx) {
    const out = ent.clone$()

    if (null == ent.id$) {
      const { id } = intern.generateid(ctx)
      out.id = id
    } else {
      out.id = ent.id$
      delete out.id$
    }

    return out
  },


  async do_create(msg, ctx) {
    const { ent } = msg
    const { seneca } = ctx

    const new_ent = intern.make_insertable(ent, ctx)
    const upsert_fields = intern.is_upsert(msg)


    let op_name
    let out

    if (null == upsert_fields) {
      op_name = 'save/insert'
      out = await intern.insertent({ ent: new_ent }, ctx)
    } else {
      op_name = 'save/upsert'
      out = await intern.upsertent(upsert_fields, { ent: new_ent }, ctx)
    }

    seneca.log.debug(op_name, 'ok', out)

    return out
  },


  async do_update(msg, ctx) {
    const { ent } = msg
    const { seneca } = ctx

    const { id: ent_id } = ent
    const entp = intern.makeentp(ent)

    const update = await intern.updateent({
      ent,
      set: intern.compact(entp),
      where: { id: ent_id }
    }, ctx)


    let out

    const updated_anything = update.affectedRows > 0

    if (updated_anything) {
      out = await intern.loadent({ ent, where: { id: ent.id } }, ctx)
    } else {
      out = await intern.insertent({ ent }, ctx)
    }

    seneca.log.debug('save/update', 'ok', out)

    return out
  },


  is_empty(obj) {
    const num_keys = Object.keys(obj).length
    return 0 === num_keys
  },


  async upsertent(upsert_fields, args, ctx) {
    const { ent } = args

    const entp = intern.makeentp(ent)
    const ent_table = intern.tablename(ent)

    return intern.transaction(async (trx) => {
      const trx_ctx = { ...ctx, db: trx }

      const update_q = upsert_fields
        .filter(c => undefined !== entp[c])
        .reduce((h, c) => {
          h[c] = entp[c]
          return h
        }, {})

      if (intern.is_empty(update_q)) {
        return intern.insertent({ ent }, trx_ctx)
      }

      const update_set = { ...entp }
      delete update_set.id

      await intern.updateent({
        ent,
        where: update_q,
        set: update_set
      }, trx_ctx)


      await intern.insertrowwherenotexists({
        into: ent_table,
        values: intern.compact(entp),
        where_not: update_q
      }, trx_ctx)

      // NOTE: Because MySQL does not support "RETURNING", we must fetch
      // the entity in a separate trip to the db. We can fetch the entity
      // by the query and not worry about duplicates - this is because
      // the query is unique by definition, because upserts can only work
      // for unique keys.
      //
      return intern.loadent({ ent, where: update_q }, trx_ctx)
    }, ctx)
  },


  where_of_q(q, ctx) {
    if ('string' === typeof q || Array.isArray(q)) {
      return { id: q }
    }

    const { seneca } = ctx

    return seneca.util.clean(q)
  },


  async selectents(args, ctx) {
    const { ent } = args
    const from = intern.tablename(ent)

    const sel_args = { ...args, from, columns: '*' }
    delete sel_args.ent

    const rows = await intern.selectrows(sel_args, ctx)
    const out = rows.map(row => intern.makeent(ent, row))

    return out
  },


  async listents(args, ctx) {
    return intern.selectents(args, ctx)
  },


  async loadent(args, ctx) {
    const load_args = { ...args, limit: 1 }
    const out = await intern.selectents(load_args, ctx)


    if (0 === out.length) {
      return null
    }

    return out[0]
  },


  async deleteent(args, ctx) {
    const { ent } = args
    const ent_table = intern.tablename(ent)

    const del_args = { ...args, from: ent_table }
    delete del_args.ent

    return await intern.deleterows(del_args, ctx)
  },


  async updateent(args, ctx) {
    const { ent } = args
    const ent_table = intern.tablename(ent)

    const update_args = { ...args, table: ent_table }
    delete update_args.ent

    return intern.updaterows(update_args, ctx)
  },


  is_native(msg) {
    const { q } = msg

    if ('string' === typeof q.native$) {
      return q.native$
    }

    if (Array.isArray(q.native$)) {
      Assert(0 < q.native$.length, 'q.native$.length')
      const [sql, ...bindings] = q.native$

      return { sql, bindings }
    }

    return null
  },


  tablename(ent) {
    const canon = ent.canon$({ object: true })
    return (canon.base ? canon.base + '_' : '') + canon.name
  },


  is_object(x) {
    const type = typeof x
    return (null != x) && ('object' === type || 'function' === type)
  },


  is_date(x) {
    return '[object Date]' === toString.call(x)
  },


  /**
   * NOTE: makeentp is used to create a new persistable entity from the entity
   * object.
   */
  makeentp(ent) {
    const fields = ent.fields$()
    const entp = {}

    for (const field of fields) {
      if (!intern.is_date(ent[field]) && intern.is_object(ent[field])) {
        entp[field] = JSON.stringify(ent[field])
      } else {
        entp[field] = ent[field]
      }
    }

    return entp
  },


  /**
   * NOTE: makeent is used to create a new entity using a row from a database.
   *
   */
  makeent(ent, row) {
    if (!row) {
      return null
    }

    const fields = Object.keys(row)
    const entp = {}

    for (const field of fields) {
      let value = row[field]

      try {
        const parsed = JSON.parse(row[field])

        if (intern.is_object(parsed)) {
          value = parsed
        }
      } catch (err) {
        if (!(err instanceof SyntaxError)) {
          throw err
        }
      }

      entp[field] = value
    }

    return ent.make$(entp)
  }
}


module.exports = { intern }
