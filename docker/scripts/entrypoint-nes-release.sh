#!/bin/bash

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
cmake -DCMAKE_BUILD_TYPE=Release -DGRPC_AVAILABLE=1 -DZMQ_AVAILABLE=1 -DCPPZMQ_AVAILABLE=1 -DNES_BUILDING_ON_CI=1 -DBoost_NO_SYSTEM_PATHS=TRUE -DBoost_INCLUDE_DIR="/usr/include" -DBoost_LIBRARY_DIR="/usr/lib/x86_64-linux-gnu" -DCPPRESTSDK_DIR="/usr/lib/x86_64-linux-gnu/cmake/" -DCMAKE_CXX_FLAGS_RELEASE:STRING="-w -Wno-unused-variable -Wno-unused-parameter -Wno-ignored-qualifiers -Wno-sign-compare " ..
# Enable this line when llvm is upgraded
#make format
make release