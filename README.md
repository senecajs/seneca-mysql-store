![Seneca](http://senecajs.org/files/assets/seneca-logo.png)
> A [Seneca.js][] data storage plugin

# seneca-mysql-store
[![Build Status][travis-badge]][travis-url]
[![Gitter][gitter-badge]][gitter-url]

[![js-standard-style][standard-badge]][standard-style]

A storage engine that uses [mySql][] to persist data. It may also be used as an example on how to
implement a storage plugin for Seneca using an underlying relational store.

- __Version:__ 0.4.0
- __Tested on:__ Seneca 0.5.0
- __Node:__ 0.10, 0.11

If you're using this module, and need help, you can:

- Post a [github issue][],
- Tweet to [@senecajs][],
- Ask on the [Gitter][gitter-url].

If you are new to Seneca in general, please take a look at [senecajs.org][]. We have everything from
tutorials to sample apps to help get you up and running quickly.

## Install
To install, simply use npm. Remember you will need to install [Seneca.js][]
separately.

```
npm install seneca
npm install seneca-mysql-store
```

## Test
To run tests, simply use npm:

```
npm run test
```

## Quick Example
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

## Usage
You don't use this module directly. It provides an underlying data storage engine for the Seneca entity API:

```js
var entity = seneca.make$('typename')
entity.someproperty = "something"
entity.anotherproperty = 100

entity.save$(function (err, entity) { ... })
entity.load$({id: ...}, function (err, entity) { ... })
entity.list$({property: ...}, function (err, entity) { ... })
entity.remove$({id: ...}, function (err, entity) { ... })
```

### Query Support
The standard Seneca query format is supported:

- `.list$({f1:v1, f2:v2, ...})` implies pseudo-query `f1==v1 AND f2==v2, ...`.

- `.list$({f1:v1, ...}, {sort$:{field1:1}})` means sort by f1, ascending.

- `.list$({f1:v1, ...}, {sort$:{field1:-1}})` means sort by f1, descending.

- `.list$({f1:v1, ...}, {limit$:10})` means only return 10 results.

- `.list$({f1:v1, ...}, {skip$:5})` means skip the first 5.

- `.list$({f1:v1,...}, {fields$:['fd1','f2']})` means only return the listed fields.

Note: you can use `sort$`, `limit$`, `skip$` and `fields$` together.

### Native Driver
As with all seneca stores, you can access the native driver, in this case, the `mysql`
`connectionPool` object using `entity.native$(function (err, connectionPool) {...})`.


## Contributing
We encourage participation. If you feel you can help in any way, be it with
examples, extra testing, or new features please get in touch.

## License
Copyright Mircea Alexandru 2015, Licensed under [MIT][].

[travis-badge]: https://travis-ci.org/mirceaalexandru/seneca-mysql-store.svg
[travis-url]: https://travis-ci.org/mirceaalexandru/seneca-mysql-store
[gitter-badge]: https://badges.gitter.im/Join%20Chat.svg
[gitter-url]: https://gitter.im/senecajs/seneca
[standard-badge]: https://raw.githubusercontent.com/feross/standard/master/badge.png
[standard-style]: https://github.com/feross/standard

[MIT]: ./LICENSE
[mySql]: https://www.mysql.com/
[node-mysqldb-native]: http://mysqldb.github.com/node-mysqldb-native/markdown-docs/queries.html
[Senecajs org]: https://github.com/senecajs/
[Seneca.js]: https://www.npmjs.com/package/seneca
[senecajs.org]: http://senecajs.org/
[github issue]: https://github.com/mirceaalexandru/seneca-mysql-store/issues
[@senecajs]: http://twitter.com/senecajs
