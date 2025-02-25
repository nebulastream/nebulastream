#!/usr/bin/bash
for selection in blocking asio; do
    for numclients in 1 10 100; do
        echo "$selection $numclients"
    done
done
