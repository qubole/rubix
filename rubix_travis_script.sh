#!/bin/bash -e

docker run -it --rm -v /var/run/docker.sock:/var/run/docker.sock -v "$PWD":/usr/src/rubix -v "$HOME/.m2":/root/.m2 $ci_env -w /usr/src/rubix rubix-build /bin/bash -c "./script.sh"