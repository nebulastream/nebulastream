# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""E53: the kernel work of one process and of its CPUs over a window, without root.

    python3 e53_kernel_snapshot.py <pid> <first cpu> <last cpu> <seconds>

Prints JSON: the change of the page faults and context switches of <pid> (/proc/<pid>/stat and status), and of every
interrupt line of /proc/interrupts summed over the CPUs <first cpu>..<last cpu>.
"""

import json
import sys
import time


def process(pid):
    with open(f"/proc/{pid}/stat") as stat:
        fields = stat.read().rsplit(")", 1)[1].split()
    values = {"minflt": int(fields[7]), "majflt": int(fields[9]), "utime_ticks": int(fields[11]), "stime_ticks": int(fields[12])}
    with open(f"/proc/{pid}/status") as status:
        for line in status:
            if line.startswith(("voluntary_ctxt_switches", "nonvoluntary_ctxt_switches")):
                name, value = line.split(":")
                values[name] = int(value)
    return values


def interrupts(first, last):
    with open("/proc/interrupts") as file:
        cpus = file.readline().split()
        columns = [index for index, name in enumerate(cpus) if first <= int(name[3:]) <= last]
        values = {}
        for line in file:
            parts = line.split()
            name = parts[0].rstrip(":")
            counts = parts[1:1 + len(cpus)]
            if len(counts) < len(cpus) or not all(count.isdigit() for count in counts):
                continue
            label = name if not name.isdigit() else name + " " + " ".join(parts[1 + len(cpus):])[:40]
            values[label] = sum(int(counts[index]) for index in columns)
    return values


def main():
    pid, first, last, seconds = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]), float(sys.argv[4])
    before_process, before_interrupts, start = process(pid), interrupts(first, last), time.time()
    time.sleep(seconds)
    after_process, after_interrupts, end = process(pid), interrupts(first, last), time.time()
    print(json.dumps({
        "seconds": round(end - start, 3),
        "process": {key: after_process[key] - before_process[key] for key in after_process},
        "interrupts": {key: after_interrupts[key] - before_interrupts.get(key, 0) for key in after_interrupts if after_interrupts[key] - before_interrupts.get(key, 0) > 0},
    }))


if __name__ == "__main__":
    main()
