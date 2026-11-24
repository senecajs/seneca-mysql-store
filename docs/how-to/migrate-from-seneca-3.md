# Migrate from Seneca 3

Goal: run an application that uses this store on Seneca 4.

1. Use Node.js 24 or later (Seneca 4 requirement).

2. Upgrade the packages. The store is now published as
   `@seneca/mysql-store`, and the entity plugin for Seneca 4 is
   `@seneca/entity`:

   ```sh
   npm uninstall seneca-mysql-store seneca-entity
   npm install seneca@^4 @seneca/entity @seneca/mysql-store
   ```

   `seneca-entity` 28 and earlier do not load on Seneca 4.0.0 (the
   error is `plugin_gubu_shape`).

3. Load `@seneca/entity` before the store, by its full name. Seneca 4
   has no built in entity support:

   ```js
   seneca
     .use('@seneca/entity', { mem_store: false })
     .use('@seneca/mysql-store', { ... })
   ```

4. Pass options with `use()` or under `options.plugin`. Seneca 4 no
   longer reads a top level `mysql-store` options block.

5. Check error handling. On Seneca 4 a failed query reaches your
   callback as the original `mysql2` error, so `err.code` is for
   example `ER_BAD_FIELD_ERROR`. Seneca 3 wrapped it; the driver error
   was `err.orig`.

6. Check authentication. The plugin now uses `mysql2`, which supports
   MySQL 8 and 9 default `caching_sha2_password` accounts. The old
   `mysql` driver needed `mysql_native_password`.
