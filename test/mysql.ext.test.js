'use strict'

var Async = require('async')
var Assert = require('chai').assert

var scratch = {}

function extendTest (settings) {
  var si = settings.seneca
  var script = settings.script

  var describe = script.describe
  var it = script.it

  describe('Extended tests', function () {
    it('Extended tests', function extended (done) {
      Async.series(
        {
          removeAll: function (next) {
            var foo = si.make({name$: 'foo'})
            foo.remove$({all$: true}, function (err, res) {
              Assert(!err)
              next()
            })
          },
          listEmpty: function (next) {
            var foo = si.make({name$: 'foo'})
            foo.list$({}, function (err, res) {
              Assert(!err)
              Assert.equal(0, res.length)
              next()
            })
          },
          insert2: function (next) {
            var foo = si.make({name$: 'foo'})
            foo.p1 = 'v1'

            foo.save$(foo, function (err, foo) {
              Assert(!err)
              Assert.isNotNull(foo.id)
              Assert.equal('v1', foo.p1)
              scratch.foo1 = foo
              next()
            })
          },
          list1: function (next) {
            scratch.foo1.list$({}, function (err, res) {
              Assert(!err)
              Assert.equal(1, res.length)
              next()
            })
          },

          list2: function (next) {
            scratch.foo1.list$({id: scratch.foo1.id}, function (err, res) {
              Assert(!err)
              Assert.equal(1, res.length)
              next()
            })
          },
          load1: function (next) {
            scratch.foo1.load$({id: scratch.foo1.id}, function (err, res) {
              Assert(!err)
              Assert.isNotNull(res.id)
              next()
            })
          },

          update: function (next) {
            scratch.foo1.p1 = 'v2'

            scratch.foo1.save$(function (err, foo) {
              Assert(!err)
              Assert.isNotNull(foo.id)
              Assert.equal('v2', foo.p1)
              next()
            })
          },

          load2: function (next) {
            scratch.foo1.load$({id: scratch.foo1.id}, function (err, res) {
              Assert(!err)
              Assert.equal('v2', res.p1)
              next()
            })
          },

          insertwithsafe: function (next) {
            var foo = si.make({name$: 'foo'})
            foo.p1 = 'v3'

            foo.save$(function (err, foo) {
              Assert(!err)
              Assert.isNotNull(foo.id)
              Assert.equal('v3', foo.p1)
              scratch.foo2 = foo
              next()
            })
          },

          list3: function (next) {
            scratch.foo2.list$({id: scratch.foo2.id}, function (err, res) {
              Assert(!err)
              Assert.equal(1, res.length)
              next()
            })
          },

          list4: function (next) {
            scratch.foo2.list$({id: scratch.foo2.id, limit$: 1}, function (err, res) {
              Assert(!err)
              Assert.equal(1, res.length)
              next()
            })
          },

          remove1: function (next) {
            scratch.foo2.remove$({id: scratch.foo2.id}, function (err) {
              Assert(!err)
              next()
            })
          },

          list5: function (next) {
            var foo = si.make('foo')
            foo.list$({}, function (err, res) {
              Assert(!err)
              Assert.equal(1, res.length)
              next()
            })
          },

          reportAllErrors: function (next) {
            const foo = si.make('foo')
            foo.missing_attribute = 'v1'


            const BAD_FIELD_ERROR_CODE = 'ER_BAD_FIELD_ERROR'
            const stdoutWrite = process.stdout.write

            process.stdout.write = output => {
              if ('string' === typeof output &&
                output.includes(BAD_FIELD_ERROR_CODE)) {
                return
              }

              return stdoutWrite.apply(process.stdout, [output])
            }


            foo.save$(function (err, foo1) {
              process.stdout.write = stdoutWrite

              Assert.isNotNull(err)
              Assert(err.message.includes(BAD_FIELD_ERROR_CODE))

              return next()
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

module.exports.extendTest = extendTest
