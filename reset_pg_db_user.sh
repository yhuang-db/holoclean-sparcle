#!/bin/bash

sudo -u yuchuanhuang psql <<PGSCRIPT
DROP DATABASE IF EXISTS holodb;
DROP USER IF EXISTS holouser;
CREATE DATABASE holodb;
CREATE USER holouser;
ALTER USER holouser WITH PASSWORD 'holopass';
GRANT ALL PRIVILEGES ON DATABASE holodb TO holouser ;
\c holodb
GRANT ALL ON SCHEMA public TO holouser;
CREATE EXTENSION postgis;
PGSCRIPT

echo "PG database and user has been created/reset."
