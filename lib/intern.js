const Q = require('./qbuilder')
const Util = require('util')
const Uuid = require('uuid')


function generateid() {
  return { id: Uuid() }
}


async function insertrow (args, ctx) {
  const query = Q.insertstm(args)
  return execquery(query, ctx)
}


async function updaterows (args, ctx) {
  const query = Q.updatestm(args)
  return execquery(query, ctx)
}


async function deleterows (args, ctx) {
  const query = Q.deletestm(args)
  return execquery(query, ctx)
}


async function selectrows (args, ctx) {
  const query = Q.selectstm(args)
  return execquery(query, ctx)
}


async function execquery (query, ctx) {
  const { db } = ctx

  // TODO: Bind `exec` to `db` here:
  //
  const exec = Util.promisify(db.query)

  if ('string' === typeof query) {
    return exec.call(db, query)
  }

  return exec.call(db, query.sql, query.bindings)
}


function is_upsert (msg) {
  const { ent, q } = msg

  if (!Array.isArray(q.upsert$)) {
    return null
  }

  const upsert_fields = q.upsert$.filter((p) => !p.includes('$'))

  if (0 === upsert_fields.length) {
    return null
  }

  return upsert_fields
}


function is_update (msg) {
  const { ent } = msg
  return null != ent.id
}


async function transaction (f, ctx) {
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
}


module.exports = {
  intern: {
    deleterows,
    execquery,
    generateid,
    insertrow,
    selectrows,
    updaterows,
    is_upsert,
    is_update,
    transaction
  }
}
