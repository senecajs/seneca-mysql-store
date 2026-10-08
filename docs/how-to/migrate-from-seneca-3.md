# Migrate from Seneca 3

Goal: run an application that uses this store on Seneca 4.

1. Upgrade the packages:

   ```sh
   npm install seneca@^4.0.0-rc5 seneca-entity@^28 @seneca/mysql-store@latest
   ```

2. Load `seneca-entity` before the store. Seneca 4 has no built in
   entity support:

   ```js
   seneca.use('entity').use('@seneca/mysql-store', { ... })
   ```

3. Pass options with `use()` or under `options.plugin`. Seneca 4 no
   longer reads a top level `mysql-store` options block.

4. Check error handling. On Seneca 4 a failed query reaches your
   callback as the original `mysql2` error, so `err.code` is for
   example `ER_BAD_FIELD_ERROR`. Seneca 3 wrapped it; the driver error
   was `err.orig`.

5. Check authentication. The plugin now uses `mysql2`, which supports
   MySQL 8 and 9 default `caching_sha2_password` accounts. The old
   `mysql` driver needed `mysql_native_password`.

6. Run on Node 22 or later (Seneca 4 requirement).
