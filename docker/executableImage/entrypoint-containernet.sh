#!/bin/bash

sleep 1 && exec $@ 2>&1 &
