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
\
-e "SSH_AUTH_SOCK=${SSH_AUTH_SOCK}" \
--mount type=bind,src="${SSH_AUTH_SOCK}",target="${SSH_AUTH_SOCK}" \
\
-v "${HOME}":/mnt/ \
-v "${HOME}/.docker_zsh_history":/home/user/.zsh_history \
-v "${CONNECTORS_SOURCE}":/home/user/connectors-source \
-v ./pysitepkg:/home/user/pysitepkg \
-v ./syncsite.sh:/home/user/syncsite.sh \
\
-v "${CONNECTORS_SOURCE}"/exec:${VENV}/usr/bin/:ro \
-v "${CONNECTORS_SOURCE}"/modules:${VENV}/lib/python3.9/site-packages/argo_connectors/:ro \
-v "${CONNECTORS_SOURCE}"/etc/customer.conf:${VENV}/etc/customer.conf:rw \
-v "${CONNECTORS_SOURCE}"/etc/global.conf:${VENV}/etc/global.conf:rw \
-v "${CONNECTORS_SOURCE}"/docker/egi-customer.conf:${VENV}/etc/egi-customer.conf:rw \
-v "${CONNECTORS_SOURCE}"/docker/eudat-customer.conf:${VENV}/etc/eudat-customer.conf:rw \
-v "${CONNECTORS_SOURCE}"/docker/ni4os-customer.conf:${VENV}/etc/ni4os-customer.conf:rw \
-v "${CONNECTORS_SOURCE}"/docker/neanias-customer.conf:${VENV}/etc/neanias-customer.conf:rw \
-v "${CONNECTORS_SOURCE}"/docker/sdc-customer.conf:${VENV}/etc/sdc-customer.conf:rw \
-v "${CONNECTORS_SOURCE}"/docker/eosc-customer-lot1prod.conf:${VENV}/etc/eosc-customer-lot1prod.conf:rw \
-v "${CONNECTORS_SOURCE}"/docker/eosc-customer-lot1stg.conf:${VENV}/etc/eosc-customer-lot1stg.conf:rw \
-v "${CONNECTORS_SOURCE}"/docker/cessda-customer.conf:${VENV}/etc/cessda-customer.conf:rw \
-v "${CONNECTORS_SOURCE}"/docker/eosccore-customer-lot1prod.conf:${VENV}/etc/eosccore-customer-lot1prod.conf:rw \
-v "${CONNECTORS_SOURCE}"/docker/eosccore-customer-lot1stg.conf:${VENV}/etc/eosccore-customer-lot1stg.conf:rw \
-v "${CONNECTORS_SOURCE}"/docker/eoscbeyond-provider-customer.conf:${VENV}/etc/eoscbeyond-provider-customer.conf:rw \
-v "${CONNECTORS_SOURCE}"/docker/global-lot1prod.conf:${VENV}/etc/global-lot1prod.conf:rw \
-v "${CONNECTORS_SOURCE}"/docker/global-lot1stg.conf:${VENV}/etc/global-lot1stg.conf:rw \
-v "${CONNECTORS_SOURCE}"/docker/global-lot1sc.conf:${VENV}/etc/global-lot1sc.conf:rw \
-v "${CONNECTORS_SOURCE}"/docker/customer-lot1sc.conf:${VENV}/etc/customer-lot1sc.conf:rw \
-v "${CONNECTORS_SOURCE}"/docker/hostcert.pem:/etc/grid-security/hostcert.pem:ro \
-v "${CONNECTORS_SOURCE}"/docker/hostkey.pem:/etc/grid-security/hostkey.pem:ro \
\
-v "${CONNECTORS_SOURCE}"/docker/combine-lot1-gocdb-sc.yml:${VENV}/etc/combine-lot1-gocdb-sc.yml:rw \
\
-v "${CONNECTORS_SOURCE}"/poetry.lock:${VENV}/poetry.lock \
-v "${CONNECTORS_SOURCE}"/pyproject.toml:${VENV}/pyproject.toml \
\
-h docker-rocky9 \
--rm -ti -v /dev/log:/dev/log ipanema:5000/connectors-r9
