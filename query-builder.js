'use strict'

var _ = require('lodash')
var MySQL = require('mysql')
var Knex = require('knex')({client: 'mysql'})

var RelationalStore = require('./lib/relational-util')
var OpParser = require('./lib/operator_parser')

var buildQueryFromExpressionPg = function (entp, query_parameters, values) {
  var params = []
  values = values || []

  if (!_.isEmpty(query_parameters) && query_parameters.params.length > 0) {
    for (var i in query_parameters.params) {
      var current_name = query_parameters.params[i]
      var current_value = query_parameters.values[i]

      var result = parseExpression(current_name, current_value)
      if (result.err) {
        return result
      }
    }

    return {err: null, data: params.join(' AND '), values: values}
  }
  else {
    return {values: values}
  }

  function parseOr (current_name, current_value) {
    if (!_.isArray(current_value)) {
      return {err: 'or$ operator requires an array value'}
    }

    var results = []
    for (var i in current_value) {
      var w = whereargsPg(entp, current_value[i])
      var current_result = buildQueryFromExpressionPg(entp, w, values)
      values = current_result.values
      results.push(current_result)
    }

    var resultStr = ''
    for (i in results) {
      if (resultStr.length > 0) {
        resultStr += ' OR '
      }
      resultStr += results[i].data
    }
    console.log('(' + resultStr + ')')
    params.push('(' + resultStr + ')')
  }

  function parseAnd (current_name, current_value) {
    if (!_.isArray(current_value)) {
      return {err: 'and$ operator requires an array value'}
    }

    var results = []
    for (var i in current_value) {
      var w = whereargsPg(entp, current_value[i])
      var current_result = buildQueryFromExpressionPg(entp, w, values)
      values = current_result.values
      results.push(current_result)
    }

    var resultStr = ''
    for (i in results) {
      if (resultStr.length > 0) {
        resultStr += ' AND '
      }
      resultStr += results[i].data
    }
    console.log('(' + resultStr + ')')
    params.push('(' + resultStr + ')')
  }

  function parseExpression (current_name, current_value) {
    if (current_name === 'or$') {
      parseOr(current_name, current_value)
    }
    else if (current_name === 'and$') {
      parseAnd(current_name, current_value)
    }
    else {
      if (current_name.indexOf('$') !== -1) {
        return {}
      }

      if (current_value === null) {
        // we can't use the equality on null because NULL != NULL
        params.push('`' + RelationalStore.escapeStr(RelationalStore.camelToSnakeCase(current_name)) + '` IS NULL')
      }
      else if (current_value instanceof RegExp) {
        var op = (current_value.ignoreCase) ? '~*' : '~'
        values.push(current_value.source)
        params.push('`' + RelationalStore.escapeStr(RelationalStore.camelToSnakeCase(current_name)) + '`' + op + '?')
      }
      else if (_.isObject(current_value)) {
        var result = parseComplexSelectOperator(current_name, current_value, params)
        if (result.err) {
          return result
        }
      }
      else {
        values.push(current_value)
        params.push('`' + RelationalStore.escapeStr(RelationalStore.camelToSnakeCase(current_name)) + '`=' + '?')
      }
    }
    return {}
  }

  function parseComplexSelectOperator (current_name, current_value, params) {
    for (var op in current_value) {
      var op_val = current_value[op]
      if (!OpParser[op]) {
        return {err: 'This operator is not yet implemented: ' + op}
      }
      var err = OpParser[op](current_name, op_val, params, values)
      if (err) {
        return {err: err}
      }
    }
    return {}
  }
}

function fixquery (qent, q) {
  var qq = {}
  for (var qp in q) {
    if (!qp.match(/\$$/)) {
      qq[qp] = q[qp]
    }
  }
  return qq
}

function whereargsPg (entp, q) {
  var w = {}

  w.params = []
  w.values = []

  var qok = RelationalStore.fixquery(entp, q)

  for (var p in qok) {
    if (qok[p] !== undefined) {
      w.params.push(RelationalStore.camelToSnakeCase(p))
      w.values.push(qok[p])
    }
  }

  return w
}

function whereargs (qent, q) {
  var w = {}
  var qok = fixquery(qent, q)

  for (var p in qok) {
    w[p] = qok[p]
  }
  return w
}

function fixPrepStatement (stm) {
  var index = 1
  while (stm.indexOf('?') !== -1) {
    stm = stm.replace('?', '$' + index)
    index++
  }
  return stm
}

function selectstm (qent, q) {
  var table = tablename(qent)
  var params = []
  var w = whereargs(RelationalStore.makeentp(qent), q)
  var wherestr = ''

  if (!_.isEmpty(w)) {
    for (var param in w) {
      params.push(param + ' = ' + MySQL.escape(w[param]))
    }
    wherestr = ' WHERE ' + params.join(' AND ')
  }

  var mq = metaquery(qent, q)
  var metastr = ' ' + mq.join(' ')

  return 'SELECT * FROM ' + table + wherestr + metastr
}

function selectstmPg (qent, q, done) {
  var specialOps = ['fields$']
  var specialOpsVal = {}

  var stm = {}

  for (var i in specialOps) {
    if (q[specialOps[i]]) {
      specialOpsVal[specialOps[i]] = q[specialOps[i]]
      delete q[specialOps[i]]
    }
  }

  var table = RelationalStore.tablename(qent)
  var entp = RelationalStore.makeentp(qent)

  var w = whereargsPg(entp, q)

  var response = buildQueryFromExpressionPg(entp, w)
  if (response.err) {
    return done(response.err)
  }

  var wherestr = response.data

  var values = response.values

  var mq = metaqueryPg(qent, q)

  var metastr = ' ' + mq.params.join(' ')

  var what = '*'
  if (specialOpsVal['fields$'] && _.isArray(specialOpsVal['fields$']) && specialOpsVal['fields$'].length > 0) {
    what = ' ' + specialOpsVal['fields$'].join(', ')
    what += ', id '
  }

  stm.text = 'SELECT ' + what + ' FROM ' + RelationalStore.escapeStr(table) + (wherestr ? ' WHERE ' + wherestr : '') + RelationalStore.escapeStr(metastr)
  stm.values = values

  done(null, stm)
}

function selectstmOrPg (qent, q) {
  var stm = {}

  var table = RelationalStore.tablename(qent)
  var entp = RelationalStore.makeentp(qent)

  var values = []
  var params = []

  // var cnt = 0

  var w = whereargsPg(entp, q.ids)

  var wherestr = ''

  if (!_.isEmpty(w) && w.params.length > 0) {
    w.params.forEach(function (param) {
      params.push('`' + RelationalStore.escapeStr(RelationalStore.camelToSnakeCase('id')) + '`=')
    })

    w.values.forEach(function (value) {
      values.push(value)
    })

    wherestr = ' WHERE ' + params.join(' OR ')
  }

  // This is required to set the limit$ to be the length of the 'ids' array, so that in situations
  // when it's not set in the query(q) it won't be applied the default limit$ of 20 records
  if (!q.limit$) {
    q.limit$ = q.ids.length
  }

  var mq = metaqueryPg(qent, q)

  var metastr = ' ' + mq.params.join(' ')

  stm.text = 'SELECT * FROM ' + RelationalStore.escapeStr(table) + wherestr + RelationalStore.escapeStr(metastr)
  stm.values = values

  return stm
}

function tablename (entity) {
  var canon = entity.canon$({object: true})
  return (canon.base ? canon.base + '_' : '') + canon.name
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

function metaqueryPg (qent, q) {
  var mq = {}

  mq.params = []
  mq.values = []

  if (q.sort$) {
    for (var sf in q.sort$) break
    var sd = q.sort$[sf] > 0 ? 'ASC' : 'DESC'
    mq.params.push('ORDER BY ' + RelationalStore.camelToSnakeCase(sf) + ' ' + sd)
  }

  if (q.limit$) {
    mq.params.push('LIMIT ' + q.limit$)
  }

  if (q.skip$) {
    mq.params.push('OFFSET ' + q.skip$)
  }

  return mq
}

function makelistquery (qent, q) {
  var query = {}
  var qf = q
  if (q.native$) {
    qf = q.native$
  }

  if (_.isArray(qf)) {
    query.text = qf[0]
    query.values = _.clone(qf)
    query.values.splice(0, 1)
    return query
  }
  else if (_.isObject(qf)) {
    return selectstm(qent, qf)
  }
  else {
    return qf
  }
}

function savestmPg (ent) {
  var stm = {}

  var table = RelationalStore.tablename(ent)
  var entp = RelationalStore.makeentp(ent)

  stm.text = Knex(table).insert(entp).toString()
  stm.values = []

  return stm
}

function updatestmPg (ent) {
  var stm = {}

  var table = RelationalStore.tablename(ent)
  var entp = RelationalStore.makeentp(ent)
  var fields = _.keys(entp)

  var values = []
  var params = []
  // var cnt = 0

  fields.forEach(function (field) {
    if (field.indexOf('$') !== -1) {
      return
    }

    if (!_.isUndefined(entp[field])) {
      values.push(entp[field])
      params.push(RelationalStore.escapeStr(RelationalStore.camelToSnakeCase(field)) + '=?')
    }
  })

  stm.text = 'UPDATE ' + RelationalStore.escapeStr(table) + ' SET ' + params + " WHERE id='" + RelationalStore.escapeStr(ent.id) + "'"
  stm.values = values

  return stm
}

function deletestm (qent, q) {
  var table = tablename(qent)
  var params = []
  var w = whereargs(RelationalStore.makeentp(qent), q)
  var wherestr = ''

  if (!_.isEmpty(w)) {
    for (var param in w) {
      params.push(param + ' = ' + MySQL.escape(w[param]))
    }
    wherestr = ' WHERE ' + params.join(' AND ')
  }

  var limistr = ''
  if (!q.all$) {
    limistr = ' LIMIT 1'
  }
  return 'DELETE FROM ' + table + wherestr + limistr
}

function deletestmPg (qent, q) {
  var stm = {}

  var table = RelationalStore.tablename(qent)
  var entp = RelationalStore.makeentp(qent)

  var values = []
  var params = []

  // var cnt = 0

  var w = whereargsPg(entp, q)

  var wherestr = ''

  if (!_.isEmpty(w) && w.params.length > 0) {
    for (var i in w.params) {
      var param = w.params[i]
      var val = w.values[i]

      if (param.indexOf('$') !== -1) {
        continue
      }

      params.push('`' + RelationalStore.escapeStr(RelationalStore.camelToSnakeCase(param)) + '`=?')
      values.push(RelationalStore.escapeStr(val))
    }

    if (params.length > 0) {
      wherestr = ' WHERE ' + params.join(' AND ')
    }
    else {
      wherestr = ' '
    }
  }

  stm.text = 'DELETE FROM ' + RelationalStore.escapeStr(table) + wherestr
  stm.values = values

  return stm
}

module.exports.fixquery = fixquery
module.exports.fixPrepStatement = fixPrepStatement
module.exports.whereargs = whereargs
module.exports.selectstm = selectstm
module.exports.selectstmPg = selectstmPg
module.exports.selectstmOrPg = selectstmOrPg
module.exports.tablename = tablename
module.exports.metaquery = metaquery
module.exports.makelistquery = makelistquery
module.exports.deletestm = deletestm
module.exports.deletestmPg = deletestmPg
module.exports.savestmPg = savestmPg
module.exports.updatestmPg = updatestmPg
