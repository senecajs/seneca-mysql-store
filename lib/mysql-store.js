/*jslint node: true */
/* Copyright (c) 2012 Mircea Alexandru */

"use strict";
var assert = require("assert");
var _ = require('lodash');
var mysql = require('mysql');
var uuid = require('node-uuid');

var NAME = "mysql-store";
var MIN_WAIT = 16;
var MAX_WAIT = 65336;
var OBJECT_TYPE = 'o';
var ARRAY_TYPE = 'a';
var DATE_TYPE = 'd';
var SENECA_TYPE_COLUMN = 'seneca';


module.exports = function(opts) {
  var seneca = this;
  var desc;
  var minwait;
  var collmap = {};
  var dbinst  = null;
  var waitmillis = MIN_WAIT;
  var spec;
  var connectionPool;

  opts.minwait = opts.minwait || MIN_WAIT;
  opts.maxwait = opts.maxwait || MAX_WAIT;



  /**
   * check and report error conditions seneca.fail will execute the callback
   * in the case of an error. Optionally attempt reconnect to the store depending
   * on error condition
   */
  function error(args, err, cb) {
    if (err) {
      seneca.log(args.tag$, 'error: ' + err);
      // seneca.fail({code:'entity/error', store: NAME, error: err}, cb);
      seneca.fail('entity/error', err, cb);

      // if (err.fatal) {
      //   if ('PROTOCOL_CONNECTION_LOST' !== err.code) {
      //     throw err;
      //   }

      //   if (MIN_WAIT === waitmillis) {
      //     collmap = {};
      //     reconnect();
      //   }
      // }
    }
    return err;
  }



  function reconnect(){
    configure(spec, function(err, me) {
      if (err) {
        seneca.log(null, 'db reconnect (wait ' + waitmillis + 'ms) failed: ' + err);
        waitmillis = Math.min(2 * waitmillis, MAX_WAIT);
        setTimeout(function() {reconnect();}, waitmillis);
      }
      else {
        waitmillis = MIN_WAIT;
        seneca.log(null,'reconnect ok');
      }
    });
  }


  /**
   * configure the store - create a new store specific connection object
   *
   * params:
   * spec - store specific configuration
   * cb - callback
   */
  function configure(specification, cb) {
    assert(specification);
    assert(cb);
    spec = specification;

    var conf = 'string' == typeof(spec) ? null : spec;
    if (!conf) {
      conf = {};
      var urlM = /^mysql:\/\/((.*?):(.*?)@)?(.*?)(:?(\d+))?\/(.*?)$/.exec(spec);
      conf.name   = urlM[7];
      conf.port   = urlM[6];
      conf.server = urlM[4];
      conf.username = urlM[2];
      conf.password = urlM[3];
      conf.port = conf.port ? parseInt(conf.port,10) : null;
    }

    var defaultConn = {
      connectionLimit: conf.poolSize || 5,
      host: conf.host,
      user: conf.user,
      password: conf.password,
      database: conf.name
    };
    var conn = conf.conn || defaultConn;
    connectionPool = mysql.createPool(conn);

    // handleDisconnect();
    connectionPool.getConnection(function(err, conn) {
      if (!error({tag$:'init'},err,cb)) {
        waitmillis = MIN_WAIT;
        if (err) {
          cb(err);
        }
        else {
          seneca.log({tag$:'init'},'db open and authed for '+conf.username);
          conn.release();
          cb(null, store);
        }
      }
      else {
        seneca.log({tag$:'init'},'db open');
        conn.release();
        cb(null, store);
      }
    });
  }



  // function handleDisconnect(cb) {
  //   connection.on( 'error', function(err) {
  //     if (!error({tag$:'init'}, err, cb) ) {
  //       waitmillis = MIN_WAIT;

  //       if (err) {
  //         cb(err);
  //       }
  //       else {
  //         seneca.log({tag$:'init'},'db open and authed');
  //         cb(null, store);
  //       }
  //     }
  //     else {
  //       seneca.log({tag$:'init'},'db open');
  //       cb(null, store);
  //     }
  //   });
  // }



  /**
   * the store interface returned to seneca
   */
  var store = {
    name: NAME,



    /**
     * close the connection
     *
     * params
     * cmd - optional close command parameters
     * cb - callback
     */
    close: function(cmd, cb) {
      assert(cb);

      if (connectionPool) {
        connectionPool.end(function(err) {
          if (err) {
            seneca.fail({code: 'connection/end', store: NAME, error: err}, cb);
          }
          cb();
        });
      }
      else {
        cb();
      }
    },



    /**
     * save the data as specified in the entitiy block on the arguments object
     *
     * params
     * args - of the form { ent: { id: , ..entitiy data..} }
     * cb - callback
     */
    save: function(args, cb) {
      assert(args);
      assert(cb);
      assert(args.ent);

      var ent = args.ent;
      var update = !!ent.id;
      var query;

      if (!ent.id) {
        if (ent.id$) {
          ent.id = ent.id$;
        } else {
          if (!opts.auto_increment) {
            ent.id = uuid();
          }
        }
      }
      var fields = ent.fields$();
      var entp = makeentp(ent);

      if (update) {
        query = 'UPDATE ' + tablename(ent) + ' SET ? WHERE id=\'' + entp.id + '\'';
        connectionPool.query(query, entp, function(err, result) {
          if (!error(args, err, cb)) {
            seneca.log(args.tag$,'save/update', result);
            cb(null, ent);
          }
        });
      }
      else {
        query = 'INSERT INTO ' + tablename(ent) + ' SET ?';
        connectionPool.query( query, entp, function( err, result ) {
          if (!error(args, err, cb)) {
            seneca.log(args.tag$, 'save/insert', result, query);

            if(opts.auto_increment && result.insertId) {
              ent.id = result.insertId;
            }

            cb(null, ent);
          }
        });
      }
    },



    /**
     * load first matching item based on id
     * params
     * args - of the form { ent: { id: , ..entitiy data..} }
     * cb - callback
     */
    load: function(args, cb) {
      assert(args);
      assert(cb);
      assert(args.qent);
      assert(args.q);

      var q = _.clone(args.q);
      var qent = args.qent;
      q.limit$ = 1;

      var query= selectstm(qent, q, connectionPool);
      connectionPool.query(query, function(err, res, fields){
        if (!error(args, err, cb)) {
          var ent = makeent( qent, res[0] );
          seneca.log(args.tag$, 'load', ent);
          cb(null, ent);
        }
      });
    },



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
    list: function(args, cb) {
      assert(args);
      assert(cb);
      assert(args.qent);
      assert(args.q);

      var qent = args.qent;
      var q = args.q;
      var queryfunc = makequeryfunc(qent, q, connectionPool);

      queryfunc(function(err, results) {
        if (!error(args, err, cb)) {
          var list = [];
          results.forEach( function(row){
            var ent = makeent(qent, row);
            list.push(ent);
          });
          cb(null, list);
        }
      });
    },



    /**
     * delete an item - fix this
     *
     * params
     * args - of the form { ent: { id: , ..entitiy data..} }
     * cb - callback
     * { 'all$': true }
     */
    remove: function(args, cb) {
      assert(args);
      assert(cb);
      assert(args.qent);
      assert(args.q);

      var qent = args.qent;
      var q = args.q;
      var query = deletestm(qent, q, connectionPool);

      connectionPool.query( query, function( err, result ) {
        if (!error(args, err, cb)) {
          cb( null, result);
        }
      });
    },



    /**
     * return the underlying native connection object
     */
    native: function(args, cb) {
      assert(args);
      assert(cb);
      assert(args.ent);

      var ent = args.ent;

      cb(null, connectionPool);
    }
  };



  /**
   * initialization
   */
  var meta = seneca.store.init(seneca, opts, store);
  desc = meta.desc;
  seneca.add({init:store.name,tag:meta.tag}, function(args,done) {
    configure(opts, function(err) {
      if (err) {
        return seneca.fail({code:'entity/configure', store:store.name, error:err, desc:desc}, done);
      }
      else done();
    });
  });

  return { name:store.name, tag:meta.tag };
};



var fixquery = function(qent, q) {
  var qq = {};
  for (var qp in q) {
    if (!qp.match(/\$$/)) {
      qq[qp] = q[qp];
    }
  }
  return qq;
};



var whereargs = function(qent,q) {
  var w = {};
  var qok = fixquery(qent,q);

  for(var p in qok) {
    w[p] = qok[p];
  }
  return w;
};



var selectstm = function(qent, q, connection) {
  var table = tablename(qent);
  var params = [];
  var w = whereargs(makeentp(qent),q);
  var wherestr = '';

  if( !_.isEmpty(w) ) {
    for(var param in w) {
      params.push(param + ' = ' + connection.escape(w[param]));
    }
    wherestr = " WHERE "+params.join(' AND ');
  }

  var mq = metaquery(qent,q);
  var metastr = ' ' + mq.join(' ');

  return "SELECT * FROM " + table + wherestr + metastr;
};



var tablename = function (entity) {
  var canon = entity.canon$({object:true});
  return (canon.base?canon.base+'_':'') + canon.name;
};



var makeentp = function(ent) {
  var entp = {};
  var fields = ent.fields$();
  var type = {};

  fields.forEach(function(field){
    if (_.isArray( ent[field])) {
      type[field] = ARRAY_TYPE;
    }
    else if (!_.isDate( ent[field]) && _.isObject( ent[field])) {
      type[field] = OBJECT_TYPE;
    }

    if (!_.isDate( ent[field]) && _.isObject(ent[field])) {
      entp[field] = JSON.stringify(ent[field]);
    }
    else {
      entp[field] = ent[field];
    }
  });

  if ( !_.isEmpty(type) ){
    entp[SENECA_TYPE_COLUMN] = JSON.stringify(type);
  }
  return entp;
};



var makeent = function(ent,row) {
  if (!row)
    return null;
  var entp;
  var fields = _.keys(row);
  var senecatype = {};

  if( !_.isUndefined(row[SENECA_TYPE_COLUMN]) && !_.isNull(row[SENECA_TYPE_COLUMN]) ){
    senecatype = JSON.parse(row[SENECA_TYPE_COLUMN]);
  }

  if( !_.isUndefined(ent) && !_.isUndefined(row) ) {
    entp = {};
    fields.forEach(function(field){
      if (SENECA_TYPE_COLUMN != field){
        if( _.isUndefined( senecatype[field]) ) {
          entp[field] = row[field];
        }
        else if (senecatype[field] == OBJECT_TYPE){
          entp[field] = JSON.parse(row[field]);
        }
        else if (senecatype[field] == ARRAY_TYPE){
          entp[field] = JSON.parse(row[field]);
        }
        else if (senecatype[field] == DATE_TYPE){
          entp[field] = new Date(row[field]);
        }
      }
    });
  }
  return ent.make$(entp);
};



var metaquery = function(qent,q) {
  var mq = [];

  if( q.sort$ ) {
    for( var sf in q.sort$ ) break;
    var sd = q.sort$[sf] < 0 ? 'DESC' : 'ASC';
    mq.push('ORDER BY '+sf+' '+sd);
  }

  if( q.limit$ ) {
    mq.push('LIMIT ' + (Number(q.limit$)||0));
  }

  if( q.skip$ ) {
    mq.push('OFFSET ' + (Number(q.skip$)||0));
  }

  return mq;
};



function makequeryfunc(qent, q, connection) {
  var qf;
  if( _.isArray( q ) ) {
    if( q.native$ ) {
      qf = function(cb) {
        var args = q.concat([cb]);
        connection.query.apply(connection, args) ;
      };
      qf.q = q;
    }
    else {
      qf = function(cb) { connection.query( q[0], _.tail(q), cb); };
      qf.q = {q:q[0],v:_.tail(q)};
    }
  }
  else if( _.isObject( q ) ) {
    if( q.native$ ) {
      var nq = _.clone(q);
      delete nq.native$;
      qf = function(cb) { connection.query( nq, cb); };
      qf.q = nq;
    }
    else {
      var query = selectstm(qent, q, connection);
      qf = function(cb) { connection.query( query, cb); };
      qf.q = query;
    }
  }
  else {
    qf = function(cb) { connection.query( q, cb); };
    qf.q = q;
  }

  return qf;
}



var deletestm = function(qent, q, connection) {
  var table = tablename(qent);
  var params = [];
  var w = whereargs(makeentp(qent), q);
  var wherestr = '';

  if (!_.isEmpty(w)) {
    for (var param in w) {
      params.push(param + ' = ' + connection.escape(w[param]));
    }
    wherestr = " WHERE "+params.join(' AND ');
  }

  var limistr = '';
  if (!q.all$) {
    limistr = ' LIMIT 1';
  }
  return "DELETE FROM " + table + wherestr + limistr;
};

