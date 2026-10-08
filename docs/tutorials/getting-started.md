# Getting started

You will connect Seneca to MySQL and store an entity. This takes about
five minutes.

## 1. Install

```sh
npm install seneca seneca-entity @seneca/mysql-store
```

The plugin needs Node 22 or later and Seneca 4.

## 2. Start a database

The repository's `docker-compose.yml` starts MySQL 9.7 on host port
33306 with the tables used here:

```sh
npm run services:up
```

Any MySQL server works if the tables exist. See
[test/support/db/seed/schema.sql](../../test/support/db/seed/schema.sql).

## 3. Write the program

This is [docs/examples/getting-started.js](../examples/getting-started.js):

```js
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
```

In your own project, write `.use('@seneca/mysql-store', {...})` instead
of the relative `require`.

## 4. Run it

```sh
node docs/examples/getting-started.js
```

Output with `seneca@4.0.0-rc5`:

```
saved apple id length 36
loaded apple 0.99
listed 1
native count 1
after remove 0
closed
```

## What happened

1. `seneca-entity` adds the entity API. The store registers the
   `sys:entity` actions behind it.
2. On init the plugin creates a `mysql2` connection pool and checks out
   one connection to verify the settings.
3. `save$` inserted a row in table `products` with a generated UUID id.
4. `native$` sent raw SQL and returned the rows as entities.
5. `close` ended the pool, so the process exits.

## Next steps

* [Use upserts and auto increment ids](../how-to/use-upserts-and-auto-increment.md)
* [Options](../reference/options.md)
* [How the store works](../explanation/how-the-store-works.md)
