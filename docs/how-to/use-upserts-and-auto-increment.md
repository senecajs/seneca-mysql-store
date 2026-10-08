# Use upserts and auto increment ids

Goal: insert a row or update the existing one matched on a unique
column, and let MySQL generate integer ids.

The full program is
[docs/examples/upsert-and-auto-increment.js](../examples/upsert-and-auto-increment.js).

## Upsert

1. Give the column a `UNIQUE` index. Without it, concurrent upserts can
   create duplicates.
2. Pass the column names in `upsert$` when saving an entity without an
   id:

   ```js
   await users
     .data$({ username: 'jimihendrix', email: 'jimi@example.com' })
     .save$({ upsert$: ['email'] })
   ```

If a row with that `email` exists it is updated and keeps its id;
otherwise a row is inserted. Fields named with `$` are ignored in
`upsert$`. If none of the listed fields has a value on the entity, a
plain insert happens.

## Auto increment ids

1. Declare the id column as `INT AUTO_INCREMENT`.
2. Save with `auto_increment$: true`. The store omits the id from the
   `INSERT` and reads back MySQL's `insertId`:

   ```js
   const row = await inc.data$({ p1: 'v1' }).save$({ auto_increment$: true })
   // typeof row.id === 'number'
   ```

`auto_increment$` also works together with `upsert$`.

Output of the example with `seneca@4.0.0-rc5`:

```
same id true username jimihendrix
rows 1
id type number
closed
```
