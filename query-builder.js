'use strict'

var _ = require('lodash')

var OBJECT_TYPE = 'o'
var ARRAY_TYPE = 'a'
var DATE_TYPE = 'd'
var SENECA_TYPE_COLUMN = 'seneca'

function fixquery (qent, q) {
  var qq = {}
  for (var qp in q) {
    if (!qp.match(/\$$/)) {
      qq[qp] = q[qp]
    }
  }
  return qq
}

function whereargs (qent, q) {
  var w = {}
  var qok = fixquery(qent, q)

  for (var p in qok) {
    w[p] = qok[p]
  }
  return w
}

function selectstm (qent, q, connection) {
  var table = tablename(qent)
  var params = []
  var w = whereargs(makeentp(qent), q)
  var wherestr = ''

  if (!_.isEmpty(w)) {
    for (var param in w) {
      params.push(param + ' = ' + connection.escape(w[param]))
    }
    wherestr = ' WHERE ' + params.join(' AND ')
  }

  var mq = metaquery(qent, q)
  var metastr = ' ' + mq.join(' ')

  return 'SELECT * FROM ' + table + wherestr + metastr
}

function tablename (entity) {
  var canon = entity.canon$({object: true})
  return (canon.base ? canon.base + '_' : '') + canon.name
}

function makeentp (ent) {
  var entp = {}
  var fields = ent.fields$()
  var type = {}

  fields.forEach(function (field) {
    if (_.isArray(ent[field])) {
      type[field] = ARRAY_TYPE
    }
    else if (!_.isDate(ent[field]) && _.isObject(ent[field])) {
      type[field] = OBJECT_TYPE
    }

    if (!_.isDate(ent[field]) && _.isObject(ent[field])) {
      entp[field] = JSON.stringify(ent[field])
    }
    else {
      entp[field] = ent[field]
    }
  })

  if (!_.isEmpty(type)) {
    entp[SENECA_TYPE_COLUMN] = JSON.stringify(type)
  }
  return entp
}

function makeent (ent, row) {
  if (!row) {
    return null
  }
  var entp
  var fields = _.keys(row)
  var senecatype = {}

  if (!_.isUndefined(row[SENECA_TYPE_COLUMN]) && !_.isNull(row[SENECA_TYPE_COLUMN])) {
    senecatype = JSON.parse(row[SENECA_TYPE_COLUMN])
  }

  if (!_.isUndefined(ent) && !_.isUndefined(row)) {
    entp = {}
    fields.forEach(function (field) {
      if (SENECA_TYPE_COLUMN !== field) {
        if (_.isUndefined(senecatype[field])) {
          entp[field] = row[field]
        }
        else if (senecatype[field] === OBJECT_TYPE) {
          entp[field] = JSON.parse(row[field])
        }
        else if (senecatype[field] === ARRAY_TYPE) {
          entp[field] = JSON.parse(row[field])
        }
        else if (senecatype[field] === DATE_TYPE) {
          entp[field] = new Date(row[field])
        }
      }
    })
  }
  return ent.make$(entp)
}

function metaquery (qent, q) {
  var mq = []

  if (q.sort$) {
    for (var sf in q.sort$) break
    var sd = q.sort$[sf] < 0 ? 'DESC' : 'ASC'
    mq.push('ORDER BY ' + sf + ' ' + sd)
  }

  if (q.limit$) {
    mq.push('LIMIT ' + (Number(q.limit$) || 0))
  }

  if (q.skip$) {
    mq.push('OFFSET ' + (Number(q.skip$) || 0))
  }

  return mq
}

function makequeryfunc (qent, q, connection) {
  var qf
  if (_.isArray(q)) {
    if (q.native$) {
      qf = function (cb) {
        var args = q.concat([cb])
        connection.query.apply(connection, args)
      }
      qf.q = q
    }
    else {
      qf = function (cb) {
        connection.query(q[0], _.tail(q), cb)
      }
      qf.q = {q: q[0], v: _.tail(q)}
    }
  }
  else if (_.isObject(q)) {
    if (q.native$) {
      var nq = _.clone(q)
      delete nq.native$
      qf = function (cb) {
        connection.query(nq, cb)
      }
      qf.q = nq
    }
    else {
      var query = selectstm(qent, q, connection)
      qf = function (cb) {
        connection.query(query, cb)
      }
      qf.q = query
    }
  }
  else {
    qf = function (cb) {
      connection.query(q, cb)
    }
    qf.q = q
  }

  return qf
}

function deletestm (qent, q, connection) {
  var table = tablename(qent)
  var params = []
  var w = whereargs(makeentp(qent), q)
  var wherestr = ''

  if (!_.isEmpty(w)) {
    for (var param in w) {
      params.push(param + ' = ' + connection.escape(w[param]))
    }
    wherestr = ' WHERE ' + params.join(' AND ')
  }

  var limistr = ''
  if (!q.all$) {
    limistr = ' LIMIT 1'
  }
  return 'DELETE FROM ' + table + wherestr + limistr
}

module.exports.fixquery = fixquery
module.exports.whereargs = whereargs
module.exports.selectstm = selectstm
module.exports.tablename = tablename
module.exports.makeentp = makeentp
module.exports.makeent = makeent
module.exports.metaquery = metaquery
module.exports.makequeryfunc = makequeryfunc
module.exports.deletestm = deletestm
