/* jslint node: true */
/* Copyright (c) 2012 Mircea Alexandru */

'use strict'

var Assert = require('assert')
var _ = require('lodash')
var MySql = require('mysql')
var Uuid = require('node-uuid')

var NAME = 'mysql-store'
var OBJECT_TYPE = 'o'
var ARRAY_TYPE = 'a'
var DATE_TYPE = 'd'
var SENECA_TYPE_COLUMN = 'seneca'

var DEFAULT_OPTIONS = {
  query_log_level: 'debug'
}

function getConnectionConfigFromString (connString) {
  var opts = /^mysql:\/\/((.*?):(.*?)@)?(.*?)(:?(\d+))?\/(.*?)$/.exec(connString)

  return {
    name: opts[7],
    port: opts[6] ? parseInt(opts[6], 10) : null,
    server: opts[4],
    username: opts[2],
    password: opts[3]
  }
}

function getConnectionConfig (params) {
  if (_.isString(params)) {
    return getConnectionConfigFromString(params)
  }

  return {
    connectionLimit: params.poolSize || 5,
    host: params.host,
    user: params.user || params.username,
    password: params.password,
    database: params.name
  }
}

module.exports = function (opts) {
  var seneca = this

  opts = seneca.util.deepextend(DEFAULT_OPTIONS, opts)

  var desc
  var _connectionPool

  /**
   * check and report error conditions seneca.fail will execute the callback
   * in the case of an error. Optionally attempt reconnect to the store depending
   * on error condition
   */
  function fail (err) {
    return seneca.fail('entity/error', err)
  }

  function testConnection (cb) {
    _connectionPool.getConnection(function (err, conn) {
      if (err) {
        return cb(err)
      }

      conn.release()
      cb(null)
    })
  }

  /**
   * setup the connection pool and try to obtain a connection
   **/
  function setup (specification, done) {
    Assert(specification)
    Assert(done)

    var conn = getConnectionConfig(specification)

    _connectionPool = MySql.createPool(conn)

    testConnection(function (err) {
      if (err) {
        return done(seneca.fail({
          code: 'entity/configure',
          store: NAME,
          error: err,
          desc: desc,
          tag$: 'init'
        }))
      }

      seneca.log({tag$: 'init'}, 'db open and authed for ' + conn.username)
      done(null)
    })
  }

  function poolQuery (query, inputs, cb) {
    var startDate = new Date()

    function report (err) {
      var log = {
        query: query,
        inputs: inputs,
        time: (new Date()) - startDate
      }

      if (log.time > 100) {
        log.tag = 'SLOW'
      }

      if (log.time > 300) {
        log.tag = 'SLOWER'
      }

      if (log.time > 500) {
        log.tag = 'SLOWEST'
      }

      if (err) {
        log.err = err
      }

      seneca.log(opts.query_log_level, 'mysql', log)

      return cb.apply(this, arguments)
    }

    if (cb === undefined) {
      cb = inputs
      inputs = undefined
    }

    if (inputs === undefined) {
      return _connectionPool.query(query, report)
    }
    else {
      return _connectionPool.query(query, inputs, report)
    }
  }

  function poolEscape () {
    return _connectionPool.escape.apply(_connectionPool, arguments)
  }

  function poolEscapeId () {
    return _connectionPool.escapeId.apply(_connectionPool, arguments)
  }

  function poolFormat () {
    return MySql.format.apply(MySql, arguments)
  }

  /**
   * close the connection
   *
   * params
   * cmd - optional close command parameters
   * cb - callback
   */
  function storeClose (cmd, cb) {
    Assert(cb)

    if (!_connectionPool) {
      return cb()
    }

    _connectionPool.end(function (err) {
      if (err) {
        return cb(seneca.fail({code: 'connection/end', store: NAME, error: err}))
      }

      cb()
    })
  }

  /**
   * save the data as specified in the entitiy block on the arguments object
   *
   * params
   * args - of the form { ent: { id: , ..entitiy data..} }
   * cb - callback
   */
  function storeSave (args, cb) {
    Assert(args)
    Assert(cb)
    Assert(args.ent)

    var ent = args.ent
    var update = !!ent.id
    var query

    if (!ent.id) {
      if (ent.id$) {
        ent.id = ent.id$
      }
      else {
        if (!opts.auto_increment) {
          ent.id = Uuid()
        }
      }
    }
    var entp = makeentp(ent)

    if (update) {
      query = 'UPDATE ' + tablename(ent) + ' SET ? WHERE id=\'' + entp.id + '\''
      connectionPool.query(query, entp, function (err, result) {
        if (err) {
          return cb(fail(err))
        }

        seneca.log(args.tag$, 'save/update', result, query)
        cb(null, ent)
      })
    }
    else {
      query = 'INSERT INTO ' + tablename(ent) + ' SET ?'
      connectionPool.query(query, entp, function (err, result) {
        if (err) {
          return cb(fail(err))
        }

        seneca.log(args.tag$, 'save/insert', result, query)

        if (opts.auto_increment && result.insertId) {
          ent.id = result.insertId
        }

        cb(null, ent)
      })
    }
  }

  /**
   * load first matching item based on id
   * params
   * args - of the form { ent: { id: , ..entitiy data..} }
   * cb - callback
   */
  function storeLoad (args, cb) {
    Assert(args)
    Assert(cb)
    Assert(args.qent)
    Assert(args.q)

    var q = _.clone(args.q)
    var qent = args.qent
    q.limit$ = 1

    var query = selectstm(qent, q, connectionPool)
    connectionPool.query(query, function (err, res, fields) {
      if (err) {
        return cb(fail(err))
      }

      var ent = makeent(qent, res[0])
      seneca.log(args.tag$, 'load', ent)
      cb(null, ent)
    })
  }

  /**
   * return a list of object based on the supplied query, if no query is supplied
   * then 'select * from ...'
   *
   * Notes: trivial implementation and unlikely to perform well due to list copy
   *        also only takes the first page of results from simple DB should in fact
   *        follow paging model
   *
   * params
   * args - of the form { ent: { id: , ..entitiy data..} }
   * cb - callback
   * a=1, b=2 simple
   * next paging is optional in simpledb
   * limit$ ->
   * use native$
   */
  function storeList (args, cb) {
    Assert(args)
    Assert(cb)
    Assert(args.qent)
    Assert(args.q)

    var qent = args.qent
    var q = args.q
    var queryfunc = makequeryfunc(qent, q, connectionPool)

    queryfunc(function (err, results) {
      if (err) {
        return cb(fail(err))
      }

      var list = _.map(results, function (row) {
        return makeent(qent, row)
      })

      cb(null, list)
    })
  }

  /**
   * delete an item - fix this
   *
   * params
   * args - of the form { ent: { id: , ..entitiy data..} }
   * cb - callback
   * { 'all$': true }
   */
  function storeRemove (args, cb) {
    Assert(args)
    Assert(cb)
    Assert(args.qent)
    Assert(args.q)

    var qent = args.qent
    var q = args.q
    var query = deletestm(qent, q, connectionPool)

    connectionPool.query(query, function (err, result) {
      if (err) {
        return cb(fail(err))
      }

      cb(null, null)
    })
  }

  /**
   * return the underlying native connection object
   */
  function storeNative (args, cb) {
    Assert(args)
    Assert(cb)
    Assert(args.ent)

    cb(null, connectionPool)
  }

  var connectionPool = {
    query: poolQuery,
    escape: poolEscape,
    escapeId: poolEscapeId,
    format: poolFormat
  }

  var store = {
    name: NAME,
    close: storeClose,
    save: storeSave,
    load: storeLoad,
    list: storeList,
    remove: storeRemove,
    native: storeNative
  }

  /**
   * initialization
   */
  var meta = seneca.store.init(seneca, opts, store)
  desc = meta.desc
  seneca.add({init: store.name, tag: meta.tag}, function (args, done) {
    setup(opts, done)
  })

  return {
    name: store.name,
    tag: meta.tag
  }
}

var fixquery = function (qent, q) {
  var qq = {}
  for (var qp in q) {
    if (!qp.match(/\$$/)) {
      qq[qp] = q[qp]
    }
  }
  return qq
}

var whereargs = function (qent, q) {
  var w = {}
  var qok = fixquery(qent, q)

  for (var p in qok) {
    w[p] = qok[p]
  }
  return w
}

var selectstm = function (qent, q, connection) {
  var table = tablename(qent)
  var params = []
  var w = whereargs(makeentp(qent), q)
  var wherestr = ''

  if (!_.isEmpty(w)) {
    for (var param in w) {
      params.push(connection.escapeId(param) + ' = ' + connection.escape(w[param]))
    }
    wherestr = ' WHERE ' + params.join(' AND ')
  }

  var mq = metaquery(qent, q)
  var metastr = ' ' + mq.join(' ')

  return 'SELECT * FROM ' + table + wherestr + metastr
}

var tablename = function (entity) {
  var canon = entity.canon$({object: true})
  return (canon.base ? canon.base + '_' : '') + canon.name
}

var makeentp = function (ent) {
  var entp = {}
  var fields = ent.fields$()
  var type = {}

  _.forEach(fields, function (field) {
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

var makeent = function (ent, row) {
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
    _.forEach(fields, function (field) {
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

var metaquery = function (qent, q) {
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

var deletestm = function (qent, q, connection) {
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
