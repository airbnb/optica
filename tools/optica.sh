#!/bin/bash
set -o xtrace

my_optica_host='https://optica.example.com'
curl --silent ${my_optica_host}/?"$1" | jq --compact-output ".nodes[] | $2"
