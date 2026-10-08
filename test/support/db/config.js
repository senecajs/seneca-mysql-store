// Connection settings for the test database. The defaults match
// docker-compose.yml (npm run services:up) and the CI service container.
const env = process.env

module.exports = {
  name: env.SENECA_TEST_MYSQL_DATABASE || 'senecatest',
  host: env.SENECA_TEST_MYSQL_HOST || '127.0.0.1',
  user: env.SENECA_TEST_MYSQL_USER || 'root',
  password: env.SENECA_TEST_MYSQL_PASSWORD || 'itsasekret_85a96vbFdh',
  port: parseInt(env.SENECA_TEST_MYSQL_PORT || '33306', 10)
}
