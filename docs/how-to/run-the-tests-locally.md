# Run the tests locally

Goal: run the full test suite against a real MySQL server.

1. Use Node 24 (or 22) and install dependencies:

   ```sh
   npm install
   ```

2. Start MySQL 9.7 (container `seneca-mysql-store-mysql`, host port
   33306). The schema in `test/support/db/seed/schema.sql` is loaded on
   first start. The command waits until the health check passes:

   ```sh
   npm run services:up
   ```

3. Run the tests:

   ```sh
   npm test
   ```

4. Optionally test against another Seneca build, then restore:

   ```sh
   npm install --no-save /path/to/seneca-4.0.0.tgz
   npm test
   npm install
   ```

5. Stop MySQL and delete its volume:

   ```sh
   npm run services:down
   ```

## Connection settings

`npm test` does not start Docker. It reads these variables; the defaults
match `docker-compose.yml` and the CI service container.

| Variable | Default |
| -------- | ------- |
| `SENECA_TEST_MYSQL_HOST` | `127.0.0.1` |
| `SENECA_TEST_MYSQL_PORT` | `33306` |
| `SENECA_TEST_MYSQL_USER` | `root` |
| `SENECA_TEST_MYSQL_PASSWORD` | `itsasekret_85a96vbFdh` |
| `SENECA_TEST_MYSQL_DATABASE` | `senecatest` |

To use your own server, create the tables from
`test/support/db/seed/schema.sql` and set the variables.

The test files are `test/mysql.test.js` (runs the shared
`seneca-store-test` suites), `test/mysql.ext.test.js` and
`test/mysql.autoincrement.test.js`.
