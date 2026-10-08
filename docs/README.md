# @seneca/mysql-store documentation

Documentation for the MySQL entity store plugin, in four sections.

## Tutorials

| Page | What you learn |
| ---- | -------------- |
| [Getting started](tutorials/getting-started.md) | Install the plugin, connect to MySQL, save, load, list and remove an entity. |

## How-to guides

| Page | Goal |
| ---- | ---- |
| [Run the tests locally](how-to/run-the-tests-locally.md) | Start MySQL with Docker and run the test suite. |
| [Use upserts and auto increment ids](how-to/use-upserts-and-auto-increment.md) | Insert or update on a unique column; let MySQL generate ids. |
| [Run native SQL](how-to/run-native-sql.md) | Send your own SQL through `list$` or use the connection pool. |
| [Migrate from Seneca 3](how-to/migrate-from-seneca-3.md) | Move an application using this store to Seneca 4. |
| [Create a release](how-to/create-a-release.md) | Maintainer release steps. |

## Reference

| Page | Contents |
| ---- | -------- |
| [Options](reference/options.md) | Connection settings and other plugin options. |
| [Messages](reference/messages.md) | Action patterns and query directives. |
| [Errors](reference/errors.md) | Error codes defined by the plugin. |

## Explanation

| Page | Topic |
| ---- | ----- |
| [How the store works](explanation/how-the-store-works.md) | Tables, column mapping, JSON values, transactions, Seneca 3 versus 4. |

## Feature index

| Feature | Kind | Documented in |
| ------- | ---- | ------------- |
| `name` | option | [Options](reference/options.md#connection-settings) |
| `host` | option | [Options](reference/options.md#connection-settings) |
| `user`, `username` | option | [Options](reference/options.md#connection-settings) |
| `password` | option | [Options](reference/options.md#connection-settings) |
| `port` | option | [Options](reference/options.md#connection-settings) |
| `poolSize` | option | [Options](reference/options.md#connection-settings) |
| `conn` | option | [Options](reference/options.md#connection-settings) |
| connection URL string | option | [Options](reference/options.md#connection-url) |
| `map` | option | [Options](reference/options.md#entity-mapping) |
| `minwait`, `maxwait`, `query_log_level`, `auto_increment`, `benchmark` | option | [Options](reference/options.md#unused-defaults) |
| `sys:entity,cmd:save` | action | [Messages](reference/messages.md#save) |
| `sys:entity,cmd:load` | action | [Messages](reference/messages.md#load) |
| `sys:entity,cmd:list` | action | [Messages](reference/messages.md#list) |
| `sys:entity,cmd:remove` | action | [Messages](reference/messages.md#remove) |
| `sys:entity,cmd:native` | action | [Messages](reference/messages.md#native) |
| close (`sys:seneca,cmd:close`) | action | [Messages](reference/messages.md#close) |
| `init:mysql-store` | action | [Messages](reference/messages.md#init) |
| `upsert$` | directive | [Messages](reference/messages.md#save) |
| `auto_increment$` | directive | [Messages](reference/messages.md#save) |
| `native$` | directive | [Messages](reference/messages.md#list) |
| `sort$`, `limit$`, `skip$` | directive | [Messages](reference/messages.md#query-directives) |
| `all$`, `load$` | directive | [Messages](reference/messages.md#remove) |
| `entity/configure` | error | [Errors](reference/errors.md) |
| `connection/end` | error | [Errors](reference/errors.md) |
| `SENECA_TEST_MYSQL_*` | test env | [Run the tests locally](how-to/run-the-tests-locally.md) |
