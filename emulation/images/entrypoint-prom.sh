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



#/bin/prometheus-node-exporter > prometheus.log 2>&1 &

#/bin/prometheus-node-exporter --no-collector.arp --no-collector.bcache --no-collector.bonding --no-collector.conntrack \
#--no-collector.edac --no-collector.entropy --no-collector.filefd --no-collector.hwmon --no-collector.infiniband \
#--no-collector.ipvs --no-collector.mdadm --no-collector.nfs --no-collector.nfsd --no-collector.pressure \
#--no-collector.sockstat --no-collector.textfile --no-collector.time --no-collector.timex --no-collector.uname \
#--no-collector.vmstat --no-collector.xfs --no-collector.zfs --no-collector.systemd --no-collector.cpu \
#--no-collector.diskstats --no-collector.loadavg --no-collector.netclass --no-collector.netstat --no-collector.stat \
#> prometheus.log 2>&1 &


if [ $1 = "crd" ]; then
  echo "Executing coordinator script:"
  sleep 20 && exec /entrypoint.sh ${@:2} > nes-runtime.log 2>&1 &
else
  echo "Executing worker script:"
  sleep 25 && exec /entrypoint.sh ${@:2} > nes-runtime.log 2>&1 &
fi
