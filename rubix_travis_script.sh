#!/bin/bash -e

docker run -it --rm -v "$PWD":/usr/src/rubix -v "$HOME/.m2":/root/.m2 $ci_env -w /usr/src/rubix quboleinc/hadoop_mvn_thrift /bin/bash script.sh