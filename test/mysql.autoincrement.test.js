'use strict'

const { expect } = require('@hapi/code')
const { make_it } = require('./support/helpers')

function autoincrementTest (settings) {
  const si = settings.seneca

  const { script } = settings
  const { describe } = script
  const it = make_it(script)

  describe('Autoincrement tests', function () {
    it('works with all$: true', function (done) {
      const foo = si.make({name$: 'incremental'})
      foo.remove$({ all$: true }, done)
    })

    it('works with all$: true', function (done) {
      const inc = si.make('incremental')
      inc.p1 = 'v1'

      inc.save$(function (err, inc1) {
        if (err) {
          return done(err)
        }

        expect(null == inc1.id).to.equal(false)

        return inc.load$({ id: inc1.id }, function (err, inc2) {
          if (err) {
            return done(err)
          }

          expect(inc2).to.contain({
            id: inc1.id,
            v1: 'v1'
          })

          expect(null == inc2).to.equal(false)

          expect(inc2).to.contain({
            id: inc1.id,
            p1: 'v1'
          })

          return done()
        })
      })
    })
  })
}

module.exports.autoincrementTest = autoincrementTest
