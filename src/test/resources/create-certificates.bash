#!/usr/bin/env bash
##################################################################################
#
# Copyright 2019 the original author or authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
##################################################################################


##################################################################################
#
# Script to generate client and server certificates to run postgresql with SSL enabled
#
##################################################################################

echo "Creating client certificate..."

docker run \
  -v "$(pwd)":/out \
  --rm \
  --entrypoint openssl \
  frapsoft/openssl \
  req -newkey rsa:2048 -nodes -keyout /out/client.key -out /out/client.crt -x509 -days 3650 -subj "/CN=test-ssl-with-cert"


echo "Creating server certificate..."

docker run \
  -v "$(pwd)":/out \
  --rm \
  --entrypoint openssl \
  frapsoft/openssl \
  req -newkey rsa:2048 -nodes -keyout /out/server.key -out /out/server.crt -x509 -days 3650 -subj "/CN=r2dbc-postgresql-test-server"
