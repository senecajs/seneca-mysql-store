# Run native SQL

Goal: run SQL that the entity query language cannot express.

## Through list$

Pass `native$` as a string, or as an array of SQL followed by bind
values (`?` placeholders). Each row becomes an entity of the queried
type.

```js
const rows = await seneca.entity('products')
  .list$({ native$: ['SELECT * FROM products WHERE price > ?', '1'] })
```

## Through the pool

`native$()` on an entity calls `sys:entity,cmd:native`, which replies
with the `mysql2` pool (callback API):

```js
seneca.entity('products').native$(function (err, pool) {
  pool.query('SELECT 1 AS one', function (err, rows) {
    console.log(rows[0].one)
  })
})
```

Do not call `pool.end()`; closing Seneca ends the pool.
