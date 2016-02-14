/* jslint node: true */
/* jslint asi: true */

'use strict'

var Assert = require('assert')
var Lab = require('lab')

var verify = function (cb, tests) {
  return function (error, out) {
    if (error) {
      return cb(error)
    }

    try {
      tests(out)
    }
    catch (ex) {
      return cb(ex)
    }

    cb()
  }
}

exports.test = function (settings) {
  var si = settings.seneca
  var script = settings.script || Lab.script()

  var describe = script.describe
  var it = script.it

  describe('Extra', function () {
    script.before(function (done) {
      var foo = si.make({name$: 'foo'})
      foo.remove$({all$: true}, done)
    })

    it('should report all errors', function reportAllErrors (done) {
      var foo = si.make('foo')
      foo.missing_attribute = 'v1'

      foo.save$(function (err) {
        Assert.ok(err)
        done()
      })
    })

    it('should support auto increment id', function allowAutoIncrementId (done) {
      var inc = si.make('incremental')
      inc.p1 = 'v1'

      inc.save$(function (err, inc1) {
        Assert.strictEqual(null, err)
        Assert.ok(inc1.id)

        inc.load$({id: inc1.id}, verify(done, function (inc2) {
          Assert.ok(inc2)
          Assert.equal(inc2.id, inc1.id)
          Assert.equal(inc2.p1, 'v1')
        }))
      })
    })
  })

  return script
}

