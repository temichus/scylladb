#!/bin/bash
# run_cmd CMD
# Assume that DRY_RUN is defined to true or false
function run_cmd {
    CMD=$*

    if $DRY_RUN ; then
        echo "Dry-run: $CMD"
    else
	echo "Running: $CMD"
        $CMD
    fi
}

# fail_if_param_missing VALUE PARAM_NAME
function fail_if_param_missing {
  value=$1
  param=$2
  if [[ "x$value" = "x" ]]; then
    echo "Error: missing mandatory parameter '$param', exiting..."
    echo ""
    usage
    exit 1
  fi
}
