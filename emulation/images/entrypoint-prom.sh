#!/bin/bash

#/bin/prometheus-node-exporter > prometheus.log 2>&1 &

/bin/prometheus-node-exporter --no-collector.arp --no-collector.bcache --no-collector.bonding --no-collector.conntrack \
--no-collector.edac --no-collector.entropy --no-collector.filefd --no-collector.hwmon --no-collector.infiniband \
--no-collector.ipvs --no-collector.mdadm --no-collector.nfs --no-collector.nfsd --no-collector.pressure \
--no-collector.sockstat --no-collector.textfile --no-collector.time --no-collector.timex --no-collector.uname \
--no-collector.vmstat --no-collector.xfs --no-collector.zfs --no-collector.systemd --no-collector.cpu \
--no-collector.diskstats --no-collector.loadavg --no-collector.netclass --no-collector.netstat --no-collector.stat \
> prometheus.log 2>&1 &


if [ $1 = "crd" ]; then
  echo "Executing coordinator script:"
  exec /entrypoint.sh ${@:2} > nes-runtime.log 2>&1 &
else
  echo "Executing worker script:"
  sleep 20 && exec /entrypoint.sh ${@:2} > nes-runtime.log 2>&1 &
fi
