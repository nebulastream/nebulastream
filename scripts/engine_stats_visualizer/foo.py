# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import plotext as plt
from time import sleep
import sys
from pathlib import Path

def print_file(filename: Path):
    data = []
    cur_wdw = 0
    cur_dat = 0
    with open(filename, "r") as file:
        while True:
            if line := file.readline():
                while not line.endswith("\n"):
                    line += file.readline()
                wdw = int(line.strip().split()[5])
                tps = int(line.strip().split()[-1])
                if (wdw != cur_wdw):
                    data.append(cur_dat)
                    cur_dat = 0
                    cur_wdw = wdw
                
                cur_dat += tps

                if len(data) > 100:
                    data = data[1:]

                plt.plot(data)
                plt.show()

                # sleep(0.2)

                plt.clear_figure()



def main():
    if len(sys.argv) != 2:
        print("Usage: give filename of EngineStats file as sole arg. File does not have to exist yet, script will wait until it is created.")
        sys.exit(1)
    path = Path(sys.argv[1])
    while not path.exists():
        sleep(0.1)
    print_file(path)

if __name__ == "__main__":
    main()
