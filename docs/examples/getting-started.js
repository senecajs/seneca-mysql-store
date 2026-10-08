// Save, load, list and remove an entity in MySQL.
// Needs the test database: npm run services:up
const Seneca = require('seneca')

const env = process.env

const seneca = Seneca({ legacy: false })
  .test()
  .use('entity')
  .use(require('../../mysql-store.js'), {
    name: env.SENECA_TEST_MYSQL_DATABASE || 'senecatest',
    host: env.SENECA_TEST_MYSQL_HOST || '127.0.0.1',
    port: parseInt(env.SENECA_TEST_MYSQL_PORT || '33306', 10),
    user: env.SENECA_TEST_MYSQL_USER || 'root',
    password: env.SENECA_TEST_MYSQL_PASSWORD || 'itsasekret_85a96vbFdh'
  })

seneca.ready(async function () {
  // The products table exists in test/support/db/seed/schema.sql.
  const product = seneca.entity('products')

  await product.remove$({ all$: true })

  const apple = await product.data$({ label: 'apple', price: '0.99' }).save$()
  console.log('saved', apple.label, 'id length', apple.id.length)

  const loaded = await product.load$(apple.id)
  console.log('loaded', loaded.label, loaded.price)

  const list = await product.list$({ label: 'apple' })
  console.log('listed', list.length)

  const rows = await product.list$({ native$: ['SELECT COUNT(*) AS n FROM products'] })
  console.log('native count', rows[0].n)

  await product.remove$(apple.id)
  console.log('after remove', (await product.list$()).length)

  seneca.close(() => console.log('closed'))
})
