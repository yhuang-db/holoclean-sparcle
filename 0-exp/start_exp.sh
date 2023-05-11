#!/usr/bin/env bash
# Set & move to home directory
source ../set_env.sh

if [ $# -eq 1 ]; then
  script="$1"
fi

echo "Launching experiment script $script"
python $script
