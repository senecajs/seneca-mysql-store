/*jslint node: true */
/* Copyright (c) 2012 Mircea Alexandru */

"use strict";
var Assert = require( "assert" );
var _ = require( 'lodash' );
var MySQL = require( 'mysql' );
var UUID = require( 'node-uuid' );
var defaultConfig = require( "./default-config.json" )

var Eraro = require( 'eraro' )( {
  package: 'mysql'
} )

var OBJECT_TYPE = 'o';
var ARRAY_TYPE = 'a';
var DATE_TYPE = 'd';
var SENECA_TYPE_COLUMN = 'seneca';

module.exports = function ( options ) {
  var seneca = this;

  var opts = seneca.util.deepextend( defaultConfig, options )
  // Declare internals
  var internals = {
    name:       'mysql-store',
    opts:       opts,
    waitmillis: opts.minwait,
    desc:       undefined,
    spec:       undefined
  }

//  internals.error = function (args, done, win) {
//    return function (err, out) {
//      if (err) {
//        seneca.log( args.tag$, 'error: ' + err );
//        throw Eraro( args.tag$, 'entity/error', err );
//      }
//      if (win) {
//        return win(out)
//      }
//    }
//  }

  internals.connectionPool = {
    query: function ( query, inputs, cb ) {
      var startDate = new Date();

      function report( err ) {

        var log = {
          query:  query,
          inputs: inputs,
          time:   (new Date()) - startDate
        };

        for ( var i in internals.benchmark.rules ) {
          if ( log.time > internals.benchmark.rules[i].time ) {
            log.tag = internals.benchmark.rules[i].tag;
          }
        }

        if ( err ) {
          log.err = err;
        }

        seneca.log( internals.opts.query_log_level, internals.name, log );

        return cb.apply( this, arguments );
      }


      if ( cb === undefined ) {
        cb = inputs;
        inputs = undefined;
      }

      if ( inputs === undefined ) {
        return internals.connectionPool.query.call( internals.connectionPool, query, report );
      }
      else {
        return internals.connectionPool.query.call( internals.connectionPool, query, inputs, report );
      }

    },

    escape: function () {
      return internals.connectionPool.escape.apply( internals.connectionPool, arguments );
    },

    format: function () {
      return MySQL.format.apply( MySQL, arguments );
    }
  };


  function reconnect() {
    configure( internals.spec, function ( err, me ) {
      if ( err ) {
        seneca.log( 'db reconnect (wait ' + internals.opts.minwait + 'ms) failed: ', err );
        internals.waitmillis = Math.min( 2 * internals.waitmillis, internals.opts.maxwait );
        setTimeout( function () {
          reconnect();
        }, internals.waitmillis );
      }
      else {
        internals.waitmillis = internals.opts.minwait;
        seneca.log( 'reconnect ok' );
      }
    } );
  }


  /**
   * configure the store - create a new store specific connection object
   *
   * params:
   * spec - store specific configuration
   * cb - callback
   */
  function configure( specification, cb ) {
    Assert( specification );
    Assert( cb );
    internals.spec = specification;

    var conf = 'string' == typeof(internals.spec) ? null : internals.spec;
    if ( !conf ) {
      conf = {};
      var urlM = /^mysql:\/\/((.*?):(.*?)@)?(.*?)(:?(\d+))?\/(.*?)$/.exec( internals.spec );
      conf.name = urlM[7];
      conf.port = urlM[6];
      conf.server = urlM[4];
      conf.username = urlM[2];
      conf.password = urlM[3];
      conf.port = conf.port ? parseInt( conf.port, 10 ) : null;
    }

    var defaultConn = {
      connectionLimit: conf.poolSize || 5,
      host:            conf.host,
      user:            conf.user || conf.username,
      password:        conf.password,
      database:        conf.name
    };
    var conn = conf.conn || defaultConn;
    internals.connectionPool = MySQL.createPool( conn );

    // handleDisconnect();
    internals.connectionPool.getConnection( function ( err, conn ) {
      if ( err ) {
        return cb(err)
      }

      internals.waitmillis = internals.opts.minwait;
      seneca.log( {tag$: 'init'}, 'db open and authed for ' + conf.username );
      conn.release();
      cb( null, store );

    } );
  }

  /**
   * the store interface returned to seneca
   */
  var store = {
    name: internals.name,

    /**
     * close the connection
     *
     * params
     * cmd - optional close command parameters
     * cb - callback
     */
    close: function ( cmd, cb ) {
      Assert( cb );

      if ( internals.connectionPool ) {
        internals.connectionPool.end( function ( err ) {
          if ( err ) {
            throw Eraro( {code: 'connection/end', store: internals.name, error: err} )
          }
          cb();
        } );
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
    save: function ( args, cb ) {
      Assert( args );
      Assert( cb );
      Assert( args.ent );

      var ent = args.ent;
      var update = !!ent.id;
      var query;

      if ( !ent.id ) {
        if ( ent.id$ ) {
          ent.id = ent.id$;
        }
        else {
          if ( !internals.opts.auto_increment ) {
            ent.id = UUID();
          }
        }
      }

      var fields = ent.fields$();
      var entp = makeentp( ent );

      if ( update ) {
        query = 'UPDATE ' + tablename( ent ) + ' SET ? WHERE id=\'' + entp.id + '\'';
        internals.connectionPool.query( query, entp, function ( err, result ) {
          if (err){
            seneca.log( args.tag$, 'save/update', err );
            return cb(err)
          }

          seneca.log( args.tag$, 'save/update', err, result );
          cb( null, ent );

        } );
      }
      else {
        query = 'INSERT INTO ' + tablename( ent ) + ' SET ?';

        internals.connectionPool.query( query, entp, function ( err, result ) {
          if (err){
            seneca.log( args.tag$, 'save/insert', err, query );
            return cb(err)
          }

          seneca.log( args.tag$, 'save/insert', err, result, query );

          if ( internals.opts.auto_increment && result.insertId ) {
            ent.id = result.insertId;
          }

          cb( null, ent );
        } );
      }
    },


    /**
     * load first matching item based on id
     * params
     * args - of the form { ent: { id: , ..entitiy data..} }
     * cb - callback
     */
    load: function ( args, cb ) {
      Assert( args );
      Assert( cb );
      Assert( args.qent );
      Assert( args.q );

      var q = _.clone( args.q );
      var qent = args.qent;
      q.limit$ = 1;

      var query = selectstm( qent, q, internals.connectionPool );
      internals.connectionPool.query( query, function ( err, res, fields ) {
        if (err){
          seneca.log( args.tag$, 'load', err );
          return cb(err)
        }

        var ent = makeent( qent, res[0] );

        seneca.log( args.tag$, 'load', ent );

        cb( null, ent );

      } );
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
    list: function ( args, cb ) {
      Assert( args );
      Assert( cb );
      Assert( args.qent );
      Assert( args.q );

      var qent = args.qent;
      var q = args.q;
      var queryfunc = makequeryfunc( qent, q, internals.connectionPool );

      queryfunc( function ( err, results ) {

        if (err){
          return cb(err)
        }

        var list = [];
        results.forEach( function ( row ) {
          var ent = makeent( qent, row );
          list.push( ent );
        } );
        cb( null, list );

      } );
    },


    /**
     * delete an item - fix this
     *
     * params
     * args - of the form { ent: { id: , ..entitiy data..} }
     * cb - callback
     * { 'all$': true }
     */
    remove: function ( args, cb ) {
      Assert( args );
      Assert( cb );
      Assert( args.qent );
      Assert( args.q );

      var qent = args.qent;
      var q = args.q;
      var query = deletestm( qent, q, internals.connectionPool );

      internals.connectionPool.query( query, function ( err, result ) {
        cb( err );
      } );
    },


    /**
     * return the underlying native connection object
     */
    native: function ( args, cb ) {
      Assert( args );
      Assert( cb );
      Assert( args.ent );

      cb( null, internals.connectionPool );
    }
  };


  /**
   * initialization
   */
  var meta = seneca.store.init( seneca, opts, store );
  internals.desc = meta.desc;
  seneca.add( {init: store.name, tag: meta.tag}, function ( args, done ) {
    configure( internals.opts, function ( err ) {
      if ( err ) {
        console.log('err: ', err)
        throw Eraro( 'entity/configure', "store: " + store.name, "error: " + err, "desc: " + internals.desc );
      }
      else done();
    } );
  } );

  return { name: store.name, tag: meta.tag };
};


var fixquery = function ( qent, q ) {
  var qq = {}
  for ( var qp in q ) {
    if ( !qp.match( /\$$/ ) ) {
      qq[qp] = q[qp]
    }
  }
  return qq
};


var whereargs = function ( qent, q ) {
  var w = {};
  var qok = fixquery( qent, q );

  for ( var p in qok ) {
    w[p] = qok[p];
  }
  return w;
};


var selectstm = function ( qent, q, connection ) {
  var table = tablename( qent );
  var params = [];
  var w = whereargs( makeentp( qent ), q );
  var wherestr = '';

  if ( !_.isEmpty( w ) ) {
    for ( var param in w ) {
      params.push( param + ' = ' + connection.escape( w[param] ) );
    }
    wherestr = " WHERE " + params.join( ' AND ' );
  }

  var mq = metaquery( qent, q );
  var metastr = ' ' + mq.join( ' ' );

  return "SELECT * FROM " + table + wherestr + metastr;
};


var tablename = function ( entity ) {
  var canon = entity.canon$( {object: true} );
  return (canon.base ? canon.base + '_' : '') + canon.name;
};


var makeentp = function ( ent ) {
  var entp = {};
  var fields = ent.fields$();
  var type = {};

  fields.forEach( function ( field ) {
    if ( _.isArray( ent[field] ) ) {
      type[field] = ARRAY_TYPE;
    }
    else if ( !_.isDate( ent[field] ) && _.isObject( ent[field] ) ) {
      type[field] = OBJECT_TYPE;
    }

    if ( !_.isDate( ent[field] ) && _.isObject( ent[field] ) ) {
      entp[field] = JSON.stringify( ent[field] );
    }
    else {
      entp[field] = ent[field];
    }
  } );

  if ( !_.isEmpty( type ) ) {
    entp[SENECA_TYPE_COLUMN] = JSON.stringify( type );
  }
  return entp;
};


var makeent = function ( ent, row ) {
  if ( !row )
    return null;
  var entp;
  var fields = _.keys( row );
  var senecatype = {};

  if ( !_.isUndefined( row[SENECA_TYPE_COLUMN] ) && !_.isNull( row[SENECA_TYPE_COLUMN] ) ) {
    senecatype = JSON.parse( row[SENECA_TYPE_COLUMN] );
  }

  if ( !_.isUndefined( ent ) && !_.isUndefined( row ) ) {
    entp = {};
    fields.forEach( function ( field ) {
      if ( SENECA_TYPE_COLUMN != field ) {
        if ( _.isUndefined( senecatype[field] ) ) {
          entp[field] = row[field];
        }
        else if ( senecatype[field] == OBJECT_TYPE ) {
          entp[field] = JSON.parse( row[field] );
        }
        else if ( senecatype[field] == ARRAY_TYPE ) {
          entp[field] = JSON.parse( row[field] );
        }
        else if ( senecatype[field] == DATE_TYPE ) {
          entp[field] = new Date( row[field] );
        }
      }
    } );
  }
  return ent.make$( entp );
};


var metaquery = function ( qent, q ) {
  var mq = [];

  if ( q.sort$ ) {
    for ( var sf in q.sort$ ) break;
    var sd = q.sort$[sf] < 0 ? 'DESC' : 'ASC';
    mq.push( 'ORDER BY ' + sf + ' ' + sd );
  }

  if ( q.limit$ ) {
    mq.push( 'LIMIT ' + (Number( q.limit$ ) || 0) );
  }

  if ( q.skip$ ) {
    mq.push( 'OFFSET ' + (Number( q.skip$ ) || 0) );
  }

  return mq;
};


function makequeryfunc( qent, q, connection ) {
  var qf;
  if ( _.isArray( q ) ) {
    if ( q.native$ ) {
      qf = function ( cb ) {
        var args = q.concat( [cb] );
        connection.query.apply( connection, args );
      };
      qf.q = q;
    }
    else {
      qf = function ( cb ) {
        connection.query( q[0], _.tail( q ), cb );
      };
      qf.q = {q: q[0], v: _.tail( q )};
    }
  }
  else if ( _.isObject( q ) ) {
    if ( q.native$ ) {
      var nq = _.clone( q );
      delete nq.native$;
      qf = function ( cb ) {
        connection.query( nq, cb );
      };
      qf.q = nq;
    }
    else {
      var query = selectstm( qent, q, connection );
      qf = function ( cb ) {
        connection.query( query, cb );
      };
      qf.q = query;
    }
  }
  else {
    qf = function ( cb ) {
      connection.query( q, cb );
    };
    qf.q = q;
  }

  return qf;
}


var deletestm = function ( qent, q, connection ) {
  var table = tablename( qent );
  var params = [];
  var w = whereargs( makeentp( qent ), q );
  var wherestr = '';

  if ( !_.isEmpty( w ) ) {
    for ( var param in w ) {
      params.push( param + ' = ' + connection.escape( w[param] ) );
    }
    wherestr = " WHERE " + params.join( ' AND ' );
  }

  var limistr = '';
  if ( !q.all$ ) {
    limistr = ' LIMIT 1';
  }
  return "DELETE FROM " + table + wherestr + limistr;
};

