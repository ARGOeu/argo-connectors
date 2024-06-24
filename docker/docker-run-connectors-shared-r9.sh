#!/bin/bash

CONNECTORS_SOURCE="${HOME}/my_work/srce/git.argo-connectors/argo-connectors"
VENV=/opt/argo-connectors/

docker rm connectors-r9; \
docker run \
--log-driver json-file \
--log-opt max-size=10m \
-u user \
--privileged \
--net host \
-v /etc/localtime:/etc/localtime:ro \
--name connectors-r9 \
-v "${HOME}":/mnt/ \
-v "${HOME}"/.ssh:/home/user/.ssh/ \
-v "${CONNECTORS_SOURCE}":/home/user/connectors-source \
\
-v "${HOME}"/my_work/srce/git.argo-connectors/docker/pysitepkg:/home/user/pysitepkg \
-v "${CONNECTORS_SOURCE}"/exec:${VENV}/usr/bin/:ro \
-v "${CONNECTORS_SOURCE}"/modules:${VENV}/lib/python3.9/site-packages/argo_connectors/:ro \
-v "${CONNECTORS_SOURCE}"/docker/egi-customer.conf:${VENV}/etc/egi-customer.conf:ro \
-v "${CONNECTORS_SOURCE}"/docker/global.conf:${VENV}/etc/global.conf:ro \
-v "${CONNECTORS_SOURCE}"/docker/hostcert.pem:/etc/grid-security/hostcert.pem:ro \
-v "${CONNECTORS_SOURCE}"/docker/hostkey.pem:/etc/grid-security/hostkey.pem:ro \
\
-v "${CONNECTORS_SOURCE}"/poetry.lock:${VENV}/poetry.lock \
-v "${CONNECTORS_SOURCE}"/pyproject.toml:${VENV}/pyproject.toml \
\
-h docker-rocky9 \
--rm -ti -v /dev/log:/dev/log ipanema:5000/connectors-r9 $1
