#!/bin/bash
set -x

#  assumes we are in the project home directory
mvn package
ret=$?
if [ $ret -ne 0 ]; then
  echo "Failed to build project"
  exit $ret
fi

PROJECT="hx-trial"

if [ -n "$1" ]; then
  if [ $1 == "STAGING" ]; then
    PROJECT="hx-data-staging"
    NEXT_PROJECT=$NEXT_PROJECT-$1
    CIRCLE_TOKEN=$CIRCLE_TOKEN_STAGING
  fi
  if [ $1 == "PRODUCTION" ]; then
    PROJECT="hx-data-production"
    NEXT_PROJECT=$NEXT_PROJECT-$1
    CIRCLE_TOKEN=$CIRCLE_TOKEN_PRODUCTION
  fi
fi

#  write out jar file versions and names to VERSIONS.txt file and save the 
#  application name to an env var
app_name=`ls -1 target/*.jar  | cut -d "/" -f 2 | tee VERSIONS.txt | grep -v original | tail -n 1 | cut -d "-" -f 1`

set +x
echo $GOOGLE_CREDENTIALS > account.json
/opt/google-cloud-sdk/bin/gcloud auth activate-service-account --key-file account.json
set -x

gsutil cp target/*.jar gs://${GSTORAGE_DEST_BUCKET}/${PROJECT}/angleddream/
ret=$?
if [ $ret -ne 0 ]; then
  echo "Failed to cp jar files to gstorage"
  exit $ret
fi

gsutil acl ch -r -u AllUsers:R gs://$GSTORAGE_DEST_BUCKET/${PROJECT}/angleddream/
curl -XPOST https://circleci.com/api/v1/project/${GITHUB_ORG}/${NEXT_PROJECT}/tree/master?circle-token=${CIRCLE_TOKEN}
