#!/bin/bash

#aber davor erstmal e2e_test_config.yaml zum Laufen bekommen


e2e_benchmark_runner=$1
if [ ! -f "$e2e_benchmark_runner" ]; then
    echo "e2eBenchmarkRunner does not exist at $e2e_benchmark_runner. Exiting now..."
    exit 1
fi

e2e_config_path="../config-examples/E2EConfigs"
cnt=1
for config_file in "$e2e_config_path"/*.yaml; do
  echo "$cnt) Running $e2e_benchmark_runner --configPath=$config_file --logPath=e2eBenchmarkRunner.log"

  $e2e_benchmark_runner --configPath=$config_file --logPath=e2eBenchmarkRunner.log >> /dev/null
  ret_val=$?
  if [ $ret_val -ne 0 ]; then
      echo "Config $config_file exists with an error"
#      exit $ret_val
  fi
  cnt=$((cnt+1))
done