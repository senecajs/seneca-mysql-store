![Seneca](http://senecajs.org/files/assets/seneca-logo.png)
> A [Seneca.js](http://senecajs.org) Data Storage Plugin

# @seneca/mysql-store

| ![Voxgig](https://www.voxgig.com/res/img/vgt01r.png) | This open source module is sponsored and supported by [Voxgig](https://www.voxgig.com). |
|---|---|


[![npm version][npm-badge]][npm-url]
[![Build Status][travis-badge]][travis-url]
[![Coverage Status][coverage-badge]][coverage-url]
[![Dependency Status][david-badge]][david-url]
[![Coveralls][BadgeCoveralls]][Coveralls]
[![Gitter][gitter-badge]][gitter-url]

## Install
To install, simply use npm. Remember you will need to install [Seneca.js][]
separately.

```
npm install seneca
npm install seneca-mysql-store
```

## Quick Example

## More Examples

See [test/](test/) for usage examples.

## Motivation

A MySQL data store plugin for the Seneca framework.

## Support

If you are having difficulty, open an issue on the GitHub repo.

## API

See [README](README.md) and Seneca docs for message patterns.

## Contributing
The [Senecajs org][] encourage open participation. If you feel you can help in any way, be it with
documentation, examples, extra testing, or new features please get in touch.

### Running tests with Docker

Build the MySQL Docker image:

```sh
npm run build

```

Start the MySQL container:
```sh
npm run start
```

Stop the MySQL container:
```sh
npm run stop
```

While the container is running you can run the tests into another terminal:
```sh
npm run test
```

#### Testing for Mac users
Before the tests can be run you must run `docker-machine env default` and copy the docker host address (example: '192.168.99.100').
This address must be inserted into the test/dbconfig.example.js file as the value for the host variable. The tests can now be run.

## Background

This plugin uses the [mysql2](https://github.com/sidorares/node-mysql2) driver.

```js
var seneca = require('seneca')()
seneca.use('mysql-store', {
  name:'senecatest',
  host:'localhost',
  user:'senecatest',
  password:'senecatest',
  port:3306
})

seneca.ready(function () {
  var apple = seneca.make$('fruit')
  apple.name  = 'Pink Lady'
  apple.price = 0.99
  apple.save$(function (err, apple) {
    console.log("apple.id = " + apple.id)
  })
})
```
