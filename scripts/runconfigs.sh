#!/usr/bin/bash
set -e -u
set -o pipefail

function configs() {
    ./scripts/genconfigs.sh # | sort -R | head -2
}

if [ -f common.csv ]; then
    mv common.csv $(date +'common.csv.bak@%s')
fi

configs | while read line; do
    {
        ./build-docker-release/nes-sources/benchmarks/nes-sources-tcp-server & \
        sleep 1 & \
        ./build-docker-release/nes-sources/benchmarks/nes-sources-benchmarks <<< "$line"
    }
    if [ ! -f common.csv ]; then
        head -n 1 results.csv | while read line; do
            echo "$line" > common.csv
        done
    fi

    tail -n -1 results.csv | while read line; do
        echo "$line" >> common.csv
    done
done
