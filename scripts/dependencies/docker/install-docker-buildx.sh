#!/bin/bash
# Copyright (C) 2021 by the NebulaStream project (https://nebula.stream)

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/bin/bash

set -ex
# This is copied from https://web.archive.org/web/20200618015712/https://mirailabs.io/blog/multiarch-docker-with-buildx/
# This needs to be run on the build server, so that it uses buildx when cross-building our images.
# This is not related to the cross-compile image, this refers to the arch of the docker image itself.
export DOCKER_CLI_EXPERIMENTAL=enabled
# Docker does not offer `latest` tag on this image
sudo docker run --rm --privileged docker/binfmt:66f9012c56a8316f9244ffd7622d7c21c1f6f28d
sudo docker buildx create --use --name nesBuilder

