#!/bin/bash
set -o xtrace

curl 2>/dev/null http://optica/?"$1" | jq --compact-output ".nodes[] | $2"
