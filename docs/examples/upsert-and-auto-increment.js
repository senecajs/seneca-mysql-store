// Upsert on a unique column, and let MySQL generate an AUTO_INCREMENT id.
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
  const users = seneca.entity('users')
  await users.remove$({ all$: true })

  // users.email has a UNIQUE index, so upsert$ on it is safe.
  const first = await users
    .data$({ username: 'jimi', email: 'jimi@example.com' })
    .save$({ upsert$: ['email'] })
  const second = await users
    .data$({ username: 'jimihendrix', email: 'jimi@example.com' })
    .save$({ upsert$: ['email'] })
  console.log('same id', first.id === second.id, 'username', second.username)
  console.log('rows', (await users.list$()).length)

  // incremental.id is INT AUTO_INCREMENT.
  const inc = seneca.entity('incremental')
  await inc.remove$({ all$: true })
  const row = await inc.data$({ p1: 'v1' }).save$({ auto_increment$: true })
  console.log('id type', typeof row.id)

  await users.remove$({ all$: true })
  await inc.remove$({ all$: true })
  seneca.close(() => console.log('closed'))
})
