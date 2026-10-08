![Seneca](http://senecajs.org/files/assets/seneca-logo.png)
> A [Seneca.js](http://senecajs.org) Data Storage Plugin

# @seneca/mysql-store

A MySQL entity store for Seneca. It implements the `seneca-entity`
store actions (save, load, list, remove, native) on MySQL tables using
the `mysql2` driver. It is tested with Seneca 4 (4.0.0-rc5 and 4.0.0)
on Node 24 and 22 against MySQL 9.7.

[![npm version][npm-badge]][npm-url]
[![build][build-badge]][build-url]

| ![Voxgig](https://www.voxgig.com/res/img/vgt01r.png) | This open source module is sponsored and supported by [Voxgig](https://www.voxgig.com). |
|---|---|

## Install

```sh
npm install seneca seneca-entity @seneca/mysql-store
```

You need a MySQL server with one table per entity type.

## Quick Example

```js
const Seneca = require('seneca')

const seneca = Seneca()
  .use('entity')
  .use('@seneca/mysql-store', {
    name: 'senecatest',
    host: '127.0.0.1',
    port: 3306,
    user: 'root',
    password: 'secret'
  })

seneca.ready(async function () {
  const apple = await seneca.entity('products')
    .data$({ label: 'apple', price: '0.99' })
    .save$()
  console.log(apple.id)
  seneca.close()
})
```

A runnable version with its output is in the
[getting started tutorial](docs/tutorials/getting-started.md).

## More Examples

* [Getting started](docs/tutorials/getting-started.md)
* [Use upserts and auto increment ids](docs/how-to/use-upserts-and-auto-increment.md)
* [Run native SQL](docs/how-to/run-native-sql.md)
* [Migrate from Seneca 3](docs/how-to/migrate-from-seneca-3.md)
* Example programs: [docs/examples](docs/examples/)

## Motivation

Seneca entities give business logic a storage independent data API.
This plugin puts those entities in MySQL, so you can switch from the
in memory store to a relational database without changing code. See
[How the store works](docs/explanation/how-the-store-works.md).

## Support

* Report problems at [GitHub issues](https://github.com/senecajs/seneca-mysql-store/issues).
* Seneca documentation: [senecajs.org](http://senecajs.org).
* This plugin is sponsored by [Voxgig](https://www.voxgig.com).

## API

| Topic | Reference |
| ----- | --------- |
| Connection settings and options | [Options](docs/reference/options.md) |
| Actions (`sys:entity,cmd:save/load/list/remove/native`) and directives (`upsert$`, `auto_increment$`, `native$`, `sort$`, `limit$`, `skip$`) | [Messages](docs/reference/messages.md) |
| Error codes (`entity/configure`, `connection/end`) | [Errors](docs/reference/errors.md) |

The full index is in [docs/README.md](docs/README.md).

## Contributing

The [Senecajs org](https://github.com/senecajs) encourages open
participation. To run the tests (Node 24 or 22, Seneca 4 prerelease as
devDependency):

```sh
npm install
npm run services:up    # MySQL 9.7 in Docker on port 33306
npm test
npm run services:down
```

Details and environment variables: [Run the tests locally](docs/how-to/run-the-tests-locally.md).
CI workflow changes are delivered as patches in [.patches](.patches/README.md);
apply them with `git am .patches/*.patch`.

## Background

Originally written by Mircea Alexandru, maintained by the Senecajs
contributors. Version 1.2 moved to the `mysql2` driver and Seneca 4.

| Seneca | Node | MySQL | Status |
| ------ | ---- | ----- | ------ |
| 4.x (4.0.0-rc5 and later) | 22, 24 | 9.7 | tested |
| 3.x | as Seneca 3 | 5.7 and later | not supported with seneca-entity 28: queries work but close does not end the pool, so the process does not exit (see GitHub issues) |

Released under the [MIT license](LICENSE).

[npm-badge]: https://img.shields.io/npm/v/@seneca/mysql-store.svg
[npm-url]: https://npmjs.com/package/@seneca/mysql-store
[build-badge]: https://github.com/senecajs/seneca-mysql-store/actions/workflows/build.yml/badge.svg
[build-url]: https://github.com/senecajs/seneca-mysql-store/actions/workflows/build.yml
