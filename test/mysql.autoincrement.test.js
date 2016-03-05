'use strict'

var Async = require('async')
var Assert = require('chai').assert

function autoincrementTest (settings) {
  var si = settings.seneca
  var script = settings.script

  var describe = script.describe
  var it = script.it

  describe('Autoincrement tests', function () {
    it('Autoincrement tests', function extended (done) {
      Async.series(
        {
          removeAll: function (next) {
            var foo = si.make({name$: 'incremental'})
            foo.remove$({all$: true}, function (err, res) {
              Assert(!err)
              next()
            })
          },
          allowAutoIncrementId: function (next) {
            var inc = si.make('incremental')
            inc.p1 = 'v1'

            inc.save$(function (err, inc1) {
              Assert.isNull(err)
              Assert.isNotNull(inc1.id)

              inc.load$({id: inc1.id}, function (err, inc2) {
                Assert.isNull(err)
                Assert.isNotNull(inc2)
                Assert.equal(inc2.id, inc1.id)
                Assert.equal(inc2.p1, 'v1')
                next()
              })
            })
          }
        },
        function (err, out) {
          Assert(!err)
          done()
        })
    })
  })
}

module.exports.autoincrementTest = autoincrementTest
