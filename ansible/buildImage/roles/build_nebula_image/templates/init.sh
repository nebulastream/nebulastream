#!/bin/bash

# compile nebula
# cd {{app_root_dir}}/nebulastream/build/
# cmake ..
# make

mkdir -p {{app_root_dir}}/nebulastream/build
cd {{app_root_dir}}/nebulastream/build
export CC=/usr/bin/clang
export CXX=/usr/bin/clang++
python3 {{app_root_dir}}/nebulastream/scripts/build/check_license.py {{app_root_dir}}/nebulastream || exit 1
cmake -DCMAKE_BUILD_TYPE=Release -DBoost_NO_SYSTEM_PATHS=TRUE -DBoost_INCLUDE_DIR="/usr/include" -DBoost_LIBRARY_DIR="/usr/lib/x86_64-linux-gnu" -DCPPRESTSDK_DIR="/usr/lib/x86_64-linux-gnu/cmake/" -DNES_USE_OPC=1 -DNES_USE_ADAPTIVE=0 ..
make -j4
cd {{app_root_dir}}/nebulastream/build/tests
ln -s ../nesCoordinator .
ln -s ../nesWorker .
make test_debug
# result=$?
# rm -rf /nebulastream/build
# exit $result

# run supervisord as main entry
exec /usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf;
