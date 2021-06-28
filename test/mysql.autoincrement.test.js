'use strict'

const { expect } = require('@hapi/code')
const { make_it } = require('./support/helpers')

function autoincrementTest (settings) {
  const { script, seneca: si } = settings

  const { describe, beforeEach, afterEach } = script
  const it = make_it(script)

  describe('Autoincrement tests', () => {
    beforeEach(() => clearDb(si))
    afterEach(() => clearDb(si))

    it('delegates id generation to the db', (done) => {
      const inc = si.make('incremental')
      inc.p1 = 'v1'

      inc.save$({ auto_increment$: true }, (err, inc1) => {
        if (err) {
          return done(err)
        }

        expect(typeof inc1.id).to.equal('number')

        return inc.load$({ id: inc1.id }, (err, inc2) => {
          if (err) {
            return done(err)
          }

          expect(inc2).to.contain({
            id: inc1.id,
            p1: 'v1'
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

    it('delegates id generation to the db, when upserting/creating', (done) => {
      const inc = si.make('incremental')
      inc.p1 = 'v1'

      inc.save$({ upsert$: ['uniq'], auto_increment$: true }, (err, inc1) => {
        if (err) {
          return done(err)
        }

        expect(typeof inc1.id).to.equal('number')

        return inc.load$({ id: inc1.id }, (err, inc2) => {
          if (err) {
            return done(err)
          }

          expect(inc2).to.contain({
            id: inc1.id,
            p1: 'v1'
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

    it('delegates id generation to the db, when upserting/matching', (done) => {
      const new_id = 37

      si.make('incremental').data$({ id: new_id, uniq: 1 }).save$((err) => {
        if (err) {
          return done(err)
        }

        return si.make('incremental')
          .data$({ p1: 'v1', uniq: 1 })
          .save$({ upsert$: ['uniq'], auto_increment$: true }, (err, inc1) => {
            if (err) {
              return done(err)
            }

            expect(inc1.id).to.equal(new_id)

            return si.make('incremental').load$({ id: inc1.id }, (err, inc2) => {
              if (err) {
                return done(err)
              }

              expect(inc2).to.contain({
                id: inc1.id,
                p1: 'v1'
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

    async function clearDb(si) {
      return new Promise((resolve, reject) => {
        const done = (err, out) => err
          ? reject(err)
          : resolve(out)

        return si.make('incremental').remove$({ all$: true }, done)
      })
    }
  })
}

module.exports.autoincrementTest = autoincrementTest
