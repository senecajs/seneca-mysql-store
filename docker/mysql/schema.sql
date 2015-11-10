DROP DATABASE IF EXISTS senecatest;
CREATE DATABASE senecatest;

USE senecatest;

CREATE USER 'senecatest'@'%' IDENTIFIED BY 'senecatest';
GRANT ALL PRIVILEGES ON senecatest.* TO 'senecatest'@'%' WITH GRANT OPTION;
CREATE USER 'root'@'%' IDENTIFIED BY 'near';
GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;

FLUSH PRIVILEGES;

CREATE TABLE foo (id VARCHAR(36), p1 VARCHAR(255), p2 VARCHAR(255), p3 VARCHAR(255), seneca VARCHAR(125));

CREATE TABLE moon_bar (
  id VARCHAR(36),
  str VARCHAR(255),
  `int` INT,
  bol BOOLEAN,
  wen TIMESTAMP,
  mark VARCHAR(255),
  `dec` REAL,
  arr TEXT,
  obj TEXT,
  seneca VARCHAR(125));

CREATE TABLE product (id VARCHAR(36), name VARCHAR(255), price INT);

CREATE TABLE incremental (
  id INT AUTO_INCREMENT,
  p1 VARCHAR(255),
  PRIMARY KEY (id)
);

