#!/usr/bin/env bash
# Set & move to home directory
source ../set_env.sh

if [ $# -eq 2 ]; then
  city="$1"
  dataset="$2"
  db_name=holodb_"$dataset"
  exp_config="$city"-"$dataset"/"$dataset"_config.toml
else
  echo "Usage: ./start_sparcle.sh <city> <dataset>"
  exit 1
fi

# init db
sudo -u "$USER" psql -d postgres <<PGSCRIPT
DROP DATABASE IF EXISTS $db_name;
CREATE DATABASE $db_name;
GRANT ALL PRIVILEGES ON DATABASE $db_name TO holouser ;
\c $db_name
GRANT ALL ON SCHEMA public TO holouser;
CREATE EXTENSION postgis;
PGSCRIPT

echo "DB $db_name created/reset."

# launch experiment
if [ "$USER" == "huan1531" ]; then
  data_dir="/home/huan1531/sparcle-exp/data/$city-$dataset"
else
  data_dir="/Users/yuchuanhuang/Github/holoclean-sparcle/testdata/0-exp-sample/$city-$dataset"
fi

echo "Launching experiment config $exp_config"

# for r = 0, 100, 500, for n = 0, 1, 2 call sparcle_driver.py
for r in 0 100 500 1000 2000; do
  for n in 0 1 2 4 8 16 32; do
    echo "Launching experiment config $exp_config with r=$r, n=$n"
    python sparcle_driver.py -d "$db_name" -t "$exp_config" -p "$data_dir" -r "$r" -n "$n"
  done
done
