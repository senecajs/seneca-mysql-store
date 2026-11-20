![Seneca](http://senecajs.org/files/assets/seneca-logo.png)
> A [Seneca.js](http://senecajs.org) Data Storage Plugin

# @seneca/mysql-store

A MySQL entity store for Seneca. It implements the `@seneca/entity`
store actions (save, load, list, remove, native) on MySQL tables using
the `mysql2` driver. It needs Seneca 4 (`seneca` `^4.0.0`), the entity
plugin `@seneca/entity` 29 and Node.js 24 or later, and is tested
against MySQL 9.7. Seneca 3 users stay on `seneca-mysql-store@1`.

[![npm version][npm-badge]][npm-url]
[![build][build-badge]][build-url]

| ![Voxgig](https://www.voxgig.com/res/img/vgt01r.png) | This open source module is sponsored and supported by [Voxgig](https://www.voxgig.com). |
|---|---|

## Install

```sh
npm install seneca@^4 @seneca/entity @seneca/mysql-store
```

You need a MySQL server with one table per entity type.

## Quick Example

```js
const Seneca = require('seneca')

const seneca = Seneca()
  .use('@seneca/entity', { mem_store: false })
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
participation. To run the tests (Node.js 24 or later):

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
contributors. Version 2.0 is the first published as
`@seneca/mysql-store`; it moved to the `mysql2` driver and Seneca 4.

| Version | Package | Seneca | Entity plugin | Node | MySQL |
| ------- | ------- | ------ | ------------- | ---- | ----- |
| 2.x | `@seneca/mysql-store` | 4 | `@seneca/entity` 29 | 24 or later | 9.7 tested |
| 1.1.x | `seneca-mysql-store` | 3 | `seneca-entity` | as Seneca 3 | 5.7 and later, `mysql` driver |

Released under the [MIT license](LICENSE).

[npm-badge]: https://img.shields.io/npm/v/@seneca/mysql-store.svg
[npm-url]: https://npmjs.com/package/@seneca/mysql-store
[build-badge]: https://github.com/senecajs/seneca-mysql-store/actions/workflows/build.yml/badge.svg
[build-url]: https://github.com/senecajs/seneca-mysql-store/actions/workflows/build.yml
