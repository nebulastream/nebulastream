#!/bin/bash
# Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

## Setting up ssh keys inside the container for pushing tag information to git repo
mkdir -p /root/.ssh/
cat /ci_secret.txt | base64 --decode > ~/.ssh/id_rsa
chmod go-r /root/.ssh/id_rsa
ssh-keyscan github.com >> ~/.ssh/known_hosts

## Setting up the git environment inside docker
cd /nebulastream
git config --global user.name "NES-CI"
git config --global user.email "nebulastream@dima.tu-berlin.de"
git config --local core.sshcommand "/usr/bin/ssh -i \"~/.ssh/id_rsa\" -o \"UserKnownHostsFile=~/.ssh/known_hosts\""

# Performing Release and formatting
mkdir -p /nebulastream/build
cd /nebulastream/build
cmake -DCMAKE_BUILD_TYPE=Release -DBoost_NO_SYSTEM_PATHS=TRUE -DBoost_INCLUDE_DIR="/usr/include" -DBoost_LIBRARY_DIR="/usr/lib/x86_64-linux-gnu" -DCPPRESTSDK_DIR="/usr/lib/x86_64-linux-gnu/cmake/" -DCMAKE_CXX_FLAGS_RELEASE:STRING="-Wall -Wno-unused-variable -Wno-unused-parameter -Wno-ignored-qualifiers -Wno-sign-compare " -DNES_USE_OPC=1 -DNES_USE_ADAPTIVE=0 ..
make version
make format
make nes-doc
make release