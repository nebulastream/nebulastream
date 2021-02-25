#!/bin/bash

# compile nebulastream
# cd {{app_root_dir}}/nebulastream/build/
# cmake ..
# make

# run supervisord as main entry
exec /usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf;
