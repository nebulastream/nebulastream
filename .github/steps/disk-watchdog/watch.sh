#!/usr/bin/env bash

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Logs free disk space every 5 seconds. Below the limit, it records what is using the disk and kills all containers,
# so that the job fails while the runner can still upload this log. A job log does not survive the runner dying.
# Kills every container on the Docker daemon, so only use this on a VM that is dedicated to the job.
log="$1"
min_free_gb="$2"

while true; do
    free_gb=$(df --output=avail -BG / | tail -n 1 | tr -dc '0-9')
    echo "$(date -u +%T) free=${free_gb}G" >> "$log"
    if [ -n "$free_gb" ] && [ "$free_gb" -lt "$min_free_gb" ]; then
        break
    fi
    sleep 5
done

{
    echo "=== free space is below ${min_free_gb}G, stopping the tests"
    # While the writers are alive: their command lines, and deleted files they hold open, which du and find cannot see.
    ps -eo pid,ppid,etimes,rss,args --sort=-rss | cut -c1-300 | head -n 60
    timeout 30 sudo -n lsof -nP +L1 2>/dev/null | sort -k7 -n | tail -n 30
    timeout 30 docker ps -a --size

    timeout 30 docker ps -q | xargs -r timeout 30 docker kill

    # The killed containers are not removed, so their files are still in the overlay directories.
    timeout 120 sudo -n find / -xdev -type f -size +200M -printf '%s %p\n' 2>/dev/null | sort -n | tail -n 40
    timeout 120 sudo -n du -xh --max-depth=6 /var/lib/docker /home/runner /tmp 2>/dev/null | sort -h | tail -n 40
    timeout 30 docker system df
    df -h /
    df -i /
} >> "$log" 2>&1
