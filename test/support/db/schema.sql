/* Copyright (c) 2012 Mircea Alexandru */
/* execute using mysql -u root < dbschema.sql */

DROP DATABASE IF EXISTS senecatest;
CREATE DATABASE senecatest;

USE senecatest;

CREATE TABLE foo (id VARCHAR(36), p1 VARCHAR(255), p2 VARCHAR(255), p3 VARCHAR(255), seneca VARCHAR(125));

CREATE TABLE moon_bar (
  id VARCHAR(36),
  str VARCHAR(255),
  `int` INT,
  bol BOOLEAN,
  wen TIMESTAMP,
  mark VARCHAR(255),
  `dec` REAL,
  arr JSON,
  obj JSON,
  seneca VARCHAR(125));

CREATE TABLE product (id VARCHAR(36), name VARCHAR(255), price INT);

CREATE TABLE incremental (
  id INT AUTO_INCREMENT,
  p1 VARCHAR(255),
  PRIMARY KEY (id)
);
