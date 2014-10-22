/* Copyright (c) Year Author, *** License */
'use strict';


var seneca = require('seneca'),
    shared = require('seneca-store-test'),
    assert = require('assert'),
    async = require('async');

var si = seneca();

//Enter your data source details
var dataSource={name:'senecatest',
                       host:'localhost',
                       user:'root',
                       password:'',
                       port:3306};

si.use(require('..'), dataSource);

si.__testcount = 0;
var testcount = 0;

describe('data-store', function(){
  // Set the timeout for your tests.
  this.timeout(30000);

  it('basic', function(done){
    testcount++;
        shared.basictest(si,done);
  });

  // Uncomment this function if you have extra tests to perform
  // it('extra', function(done){
  //   testcount++;
  //   extraTest(si,done);
  // });

  it('close', function(done){
    shared.closetest(si,testcount,done);
  });
});


// Add your extra tests to async.series array
function extraTest(si, done){
  console.log('Extra');
  assert.notEqual(si, null);

  async.series(
  [

  ],

  function(err, results) {
    err = err || null;
    if(err) {
      console.dir(err);
    }
    si.__testcount++;
    assert.equal(err, null);
    done && done();
  });
}

