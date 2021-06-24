const Q = require('./qbuilder')


function insertrow (args, ctx, done) {
  const query = Q.insertstm(args)
  return execquery(query, ctx, done)
}


function updaterows (args, ctx, done) {
  const query = Q.updatestm(args)
  return execquery(query, ctx, done)
}


function deleterows (args, ctx, done) {
  const query = Q.deletestm(args)
  return execquery(query, ctx, done)
}


function selectrows (args, ctx, done) {
  const query = Q.selectstm(args)
  return execquery(query, ctx, done)
}


function execquery (query, ctx, done) {
  const { db } = ctx

  if ('string' === typeof query) {
    return db.query(query, done)
  }

  return db.query(query.sql, query.bindings, done)
}


module.exports = {
  intern: { insertrow, updaterows, deleterows, selectrows, execquery }
}
