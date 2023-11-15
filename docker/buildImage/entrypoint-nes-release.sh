#!/bin/sh

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#quit if command returns non-zero code
set -e

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

# Performing Tag Release and formatting
mkdir -p /nebulastream/build
cd /nebulastream/build
# build the project
cmake -DCMAKE_BUILD_TYPE=Release -DNES_SELF_HOSTING=1 -DNES_USE_OPC=0 -DNES_ENABLE_EXPERIMENTAL_EXECUTION_ENGINE=1 -DNES_ENABLE_EXPERIMENTAL_EXECUTION_MLIR=1 -DNES_ENABLE_EXPERIMENTAL_EXECUTION_JNI=1 -DNES_USE_MQTT=1 -DNES_USE_ADAPTIVE=0 -DNES_BUILD_PLUGIN_ONNX=1 -DNES_BUILD_PLUGIN_TENSORFLOW=1 -DNES_BUILD_PLUGIN_ARROW=1 -DNES_USE_S2=1 ..
# fix format issues
make format
# build documentation
make nes-doc

# release the tag and push next snapshot version
if [ $RELEASE_TYPE = 'Major' ]; then
  make major_release
elif [ $RELEASE_TYPE = 'Minor' ]; then
  make minor_release
else
  make release
fi