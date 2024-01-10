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
#set -e

if ! [  -f "/opencv/CMakeLists.txt" ]; then
  echo "Please mount source code at /opencv point. Run [docker run -v <path-to-nes>:/opencv -d <build-image>]"
  exit 1
fi

if [ $# -eq 0 ]
then
    # Build OpenCV
    ccache --set-config=cache_dir=/cache_dir/
    ccache -M 10G
    cmake --fresh -B /build_dir -DWITH_PROTOBUF=OFF -DBUILD_JAVA=ON /opencv/
    cmake --build /build_dir -j12
    cd /build_dir
    rm *deb
    cpack
else
    exec $@
fi
