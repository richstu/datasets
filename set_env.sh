#!/bin/bash
export JB_DATASETS_DIR=$(dirname $(readlink -e "$BASH_SOURCE"))
export PATH=$JB_DATASETS_DIR/bin::$PATH
export PYTHONPATH=$JB_DATASETS_DIR/libs:$PYTHONPATH
