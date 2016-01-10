#!/bin/bash

#  assumes we are in the project home directory
mvn package
ret=$?
if [ $ret != 0 ]; then
  echo "Failed to build project"
  exit $ret
fi

#  write out jar file versions and names to VERSIONS.txt file and save the 
#  application name to an env var
app_name=`ls -1 target/*.jar  | cut -d "/" -f 2 | tee VERSIONS.txt | grep -v original | tail -n 1 | cut -d "-" -f 1`

gsutil cp target/*.jar gs://build-artifacts-public-eu/${app_name}
gsutil cp VERSIONS.txt gs://build-artifacts-public-eu/${app_name}
curl -XPOST https://circleci.com/api/v1/project/22acacia/pipeline-examples/tree/master?circle-token=$CIRCLE_TOKEN