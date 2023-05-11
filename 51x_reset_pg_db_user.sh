#!/bin/bash

sudo -u huan1531 psql -d postgres <<PGSCRIPT
DROP DATABASE IF EXISTS holodb;
DROP DATABASE IF EXISTS holodb_hc;
DROP DATABASE IF EXISTS holodb_sparcle;
DROP DATABASE IF EXISTS holodb_eqw;
DROP USER IF EXISTS holouser;
CREATE DATABASE holodb;
CREATE USER holouser;
ALTER USER holouser WITH PASSWORD 'holopass';
GRANT ALL PRIVILEGES ON DATABASE holodb TO holouser ;
\c holodb
CREATE EXTENSION postgis;
PGSCRIPT

echo "PG database and user has been created/reset."

sudo -u huan1531 psql -d postgres <<PGSCRIPT
CREATE DATABASE holodb_hc;
GRANT ALL PRIVILEGES ON DATABASE holodb_hc TO holouser ;
\c holodb_hc
CREATE EXTENSION postgis;
PGSCRIPT

echo "holodb_hc and user has been created/reset."

sudo -u huan1531 psql -d postgres <<PGSCRIPT
CREATE DATABASE holodb_sparcle;
GRANT ALL PRIVILEGES ON DATABASE holodb_sparcle TO holouser ;
\c holodb_sparcle
CREATE EXTENSION postgis;
PGSCRIPT

echo "holodb_sparcle and user has been created/reset."

sudo -u huan1531 psql -d postgres <<PGSCRIPT
CREATE DATABASE holodb_eqw;
GRANT ALL PRIVILEGES ON DATABASE holodb_eqw TO holouser ;
\c holodb_eqw
CREATE EXTENSION postgis;
PGSCRIPT

echo "holodb_eqw and user has been created/reset."
