# Changes

## 2.0.0 2026-11-20

The first version published as `@seneca/mysql-store`; versions up to
1.1.0 were published as `seneca-mysql-store`. 1.2.0 was prepared but
not published; its changes, among them the move from the `mysql` to the
`mysql2` driver, are part of this version.

* Seneca 4: the peer dependency is `seneca` `^4.0.0`. Seneca 3 users
  stay on `seneca-mysql-store@1`.
* Renamed from `seneca-mysql-store` to `@seneca/mysql-store`.
* Node.js 24 or later (`engines.node` `>=24`).
* The entity peer dependency is `@seneca/entity` `^29.0.0` (was
  `seneca-entity` `>=28`), the entity plugin for Seneca 4. Load it before
  the store, by its full name. The store registers through its
  `entity/init` export; the `seneca.store.init` fallback for Seneca 3 is
  removed.
* `seneca.util.deep` replaces `seneca.util.deepextend`, which Seneca 4
  deprecates (D2); they are the same function.
* Generated ids come from `crypto.randomUUID()`, still a random UUID v4.
  Removed the runtime dependencies `uuid` 3.4.0 (deprecated) and
  `eraro` 0.4.1 (unused).
* `files` no longer names `query-builder.js`, which does not exist.
* Tests use `@seneca/entity` 29 and `@seneca/store-test` 7, loaded by
  full name (B14), and no longer set the `default_plugins` option, which
  Seneca 4 does not read.

## 1.2.0 2026/10/08 (not published)
* Seneca 4 prerelease support (4.0.0-rc5 and 4.0.0): the store
  initializer now comes from seneca-entity (`entity/init`).
* Driver changed from `mysql` to `mysql2`, which authenticates against
  MySQL 8 and 9 default accounts. Query errors carry mysql2 messages.
* Upsert transactions are retried on `ER_LOCK_DEADLOCK`.
* Tested on Node 24 and 22 against MySQL 9.7; `docker-compose.yml` with
  `npm run services:up` / `services:down`.
* Tests run on @hapi/lab 26 and seneca-store-test 6; lint removed from
  `npm test`. Removed Travis, coveralls, pre-commit.
* Documentation reorganized into docs/ (tutorials, how-to, reference,
  explanation).

## 1.1.0 2016/08/27
* Added Seneca 3 and Node 6 support
* Dropped Node 0.10, 0.12, 5 support
* Updated dependencies

## 1.0.1 2016/08/05
* Fixed incorrect package.json "files" section

## 1.0.0 2016/08/04
* Replaced query generation with seneca-standard-query
* Extended and unified the patterns with seneca-postgres-store
* Update dependencies


## 0.4.1 2015/11/10
* Updated for seneca 0.7.x
* Cleanup code
* Improve tests
* Dockerize tests

## 0.4.0 2015/02/04
* Updated for seneca 0.6.1

## 0.3.0 2015/01/30
* Add query reporting including SLOW, SLOWER, SLOWEST tags
* Update version references
* Add connection pooling
* Fix sort$, add skip$
* SQL injection protection
* Add standard store-test

## 0.2.0 2014/10/24
* Do not add metadata for date columns

## 0.1.0 2014/10/24
* Use seneca-store-test and make them pass
* Add support for auto_increment columns (use auto_increment: true option)
