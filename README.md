seneca-mysql-store
==================

MySQL database layer for Seneca framework

Current Version: 0.1.0

Tested on: Node 0.10.32, Seneca 0.5.21

Tests
-----
Prerequisite:
* Must have MySQL installed
* Go to the /scripts directory and setup the test DB by running the schema.sql script in there:
  * `mysql -u "root" "-pXXXXXXX" < "schema.sql"`
  * Replace XXXXXXX with your password and root with your username as appropriate.
* Configure your username/password/database for the DB in the tests.
  * Do this by copying the dbconfig.example.js file in the /test/ directory to dbconfig.mine.js
  * Change the values to match those of your MySQL connection

Acknowledgements
----------------

This project was sponsored by [nearForm](http://nearform.com).

