# Options

Options are passed with `seneca.use('@seneca/mysql-store', options)` (or
under `options.plugin`). They are merged over `default_config.json`.

## Connection settings

Read in `configure()` in `mysql-store.js` and passed to
`mysql2.createPool()`.

| Option | Type | Default | Effect |
| ------ | ---- | ------- | ------ |
| `name` | string | none | Database name (`database` for the pool). |
| `host` | string | none (`mysql2` uses `localhost`) | Server host. |
| `user` | string | none | User name. `username` is accepted when `user` is absent. |
| `password` | string | none | Password. |
| `port` | number | `3306` | Server port. |
| `poolSize` | number | `5` | Pool `connectionLimit`. |
| `conn` | object | none | If set, passed to `mysql2.createPool()` as is, and all settings above are ignored. Use it for any other `mysql2` pool option (TLS, timezone, ...). |

On init the plugin checks out one connection. If that fails, init fails
with [`entity/configure`](errors.md).

## Connection URL

The options can also be a string of the form
`mysql://user:password@host:port/database`. The current parser has known
defects (the host is stored as `server` and the port is not parsed), so
prefer the object form.

## Entity mapping

| Option | Type | Default | Effect |
| ------ | ---- | ------- | ------ |
| `map` | object | all entities | Handled by `seneca-entity`: maps entity canons to this store, for example `{ '-/-/incremental': '*' }`. Lets several stores serve different entities. |

## Unused defaults

`default_config.json` also defines `minwait` (16), `maxwait` (65336),
`query_log_level` (`'debug'`), `auto_increment` (`false`) and
`benchmark.rules`. The current code does not read them. Use the
`auto_increment$` save directive instead of the `auto_increment` option.
