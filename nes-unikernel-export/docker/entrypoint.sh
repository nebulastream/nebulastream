#!/bin/bash
# trap ctrl-c and call ctrl_c()
trap ctrl_c INT

function ctrl_c() {
        echo "** Trapped CTRL-C"
}

NesUnikernelExporter /input/input.yaml --yaml /output/export.yaml --output /output/
