function getConfig() {
  if (process.env.CI) {
    return {
      name: 'senecatest_ci_578gw9f6wf7',
      host: 'localhost',
      user: 'root',
      password: 'itsasekret_ci_6g9b75t2gt528az',
      port: 3306
    }
  }

  return {
    name: 'senecatest',
    host: 'localhost',
    user: 'root',
    password: 'itsasekret_85a96vbFdh',
    port: 3306
  }
}

module.exports = getConfig()
