# Errors

Defined in `module.exports.errors` in `mysql-store.js` and raised with
`seneca.fail()`.

| Code | When | Details |
| ---- | ---- | ------- |
| `entity/configure` | Init could not get a connection from the pool. | `store`, `error`, `desc` |
| `connection/end` | Ending the pool on close failed. | `store`, `error` |

Query failures are not wrapped in a plugin code. They reach the caller
as the `mysql2` error (`err.code`, `err.errno`, `err.sqlMessage`) on
Seneca 4, or wrapped by Seneca with the driver error in `err.orig` on
Seneca 3.
