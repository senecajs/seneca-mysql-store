# Messages

The store registers the standard `seneca-entity` store actions. You
normally call them through entity methods (`save$`, `load$`, ...). Each
message carries `ent` or `qent` (the entity) and `q` (the query). When
`map` is set, the patterns also include `zone`, `base` and `name`.

## init

`init:mysql-store` creates the pool and verifies one connection. Fails
with `entity/configure`.

## save

`sys:entity,cmd:save`. Reply: the saved entity, reloaded from the table.

* Entity with `id`: `UPDATE ... WHERE id = ?`. If no row changed, the
  entity is inserted with that id.
* Entity without `id`: insert with `id$` if given, otherwise a UUID v4.
* `q.upsert$`: array of column names. Update the row matching those
  columns or insert one, in a transaction. See
  [Use upserts](../how-to/use-upserts-and-auto-increment.md).
* `q.auto_increment$: true`: omit the id and use MySQL's `insertId`.

Errors: the `mysql2` error, for example `ER_BAD_FIELD_ERROR` for an
unknown column.

## load

`sys:entity,cmd:load`. `q` is an id, or an object of column values
(`AND`; an array value means `IN`, `null` means `IS NULL`). Reply: the first matching entity or
`null`. Uses `sort$` and `skip$`.

## list

`sys:entity,cmd:list`. Reply: array of entities matching `q`.

* `q.native$`: SQL string, or `[sql, ...bindings]`. The rows are
  returned as entities and other query fields are ignored.

## remove

`sys:entity,cmd:remove`. Removes the first match of `q`.

* `q.all$: true`: remove all matches (respects `limit$`, `skip$`, `sort$`).
* `q.load$: true`: reply with the removed entity (single remove only).

Reply: `null` unless `load$` is set.

## native

`sys:entity,cmd:native`. Reply: the `mysql2` pool.

## close

Registered by `seneca-entity` on `sys:seneca,cmd:close`. Ends the pool
when Seneca 4 closes. Fails with
`connection/end`.

## Query directives

| Directive | Effect |
| --------- | ------ |
| `sort$` | Object `{ column: 1 }` (ascending) or `{ column: -1 }` (descending); several keys are allowed. |
| `limit$` | Maximum rows (`list`, `remove` with `all$`). |
| `skip$` | Rows to skip. |
