# How the store works

## Tables and columns

An entity `base/name` lives in table `base_name`, or `name` without a
base. The zone is ignored. Each entity field is a column of the same
name, so the tables must exist before use; the store does not create
or migrate them. The `id` column holds a UUID string unless you use
`auto_increment$`.

## Values

Object and array values (except `Date`) are written with
`JSON.stringify`. On read, every column value that parses as JSON to an
object or array becomes that object. A `JSON` or `TEXT` column works.

## Driver and pool

The plugin uses `mysql2` with its callback API and one pool per plugin
instance. `mysql2` supports the `caching_sha2_password` default of
MySQL 8 and later, which the older `mysql` driver did not.

## Upserts and transactions

MySQL has no `RETURNING` clause, so an upsert is a transaction of three
statements: `UPDATE` on the upsert columns, `INSERT ... WHERE NOT EXISTS`,
then a `SELECT` to read the row back. Under concurrent upserts on the
same key MySQL may abort one transaction with `ER_LOCK_DEADLOCK`; the
store restarts it, up to five attempts, as the MySQL manual recommends.
A `UNIQUE` index on the upsert columns is still required for
correctness.

## Seneca 3 and Seneca 4

The store obtains the entity store initializer from
`seneca.export('entity/init')`, which `seneca-entity` provides. Seneca 3
also had a core `seneca.store` decoration, used as a fallback. Close is
wired by `seneca-entity` 28 on `sys:seneca,cmd:close`. On Seneca 3.38
with `seneca-entity` 28 that hook is not reached, the pool stays open
and the process does not exit, so this release is tested on Seneca 4
only. On
Seneca 4 query errors reach callers unwrapped.
