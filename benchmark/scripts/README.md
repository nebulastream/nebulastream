# Usage for run_and_plot_benchmarks.py

usage: run_and_plot_benchmarks.py [-h] [-b BENCHMARKNAMES [BENCHMARKNAMES ...]] [-f BENCHMARKFOLDER] [-nc] [-m RUNMESSAGE] [-d]
                                  [-r RESULTFOLDER] [-jp JUSTPLOT] [-jpf JUSTPLOTFILE] [-jrb JUSTRUNBENCHMARK]
                                  [-ba BENCHMARKWITHARGS [BENCHMARKWITHARGS ...]]

optional arguments:
  -h, --help            show this help message and exit
  -b BENCHMARKNAMES [BENCHMARKNAMES ...], --benchmarks BENCHMARKNAMES [BENCHMARKNAMES ...]
                        Name of benchmark executables that should be run, if none then all benchmark executables are used. Regex is
                        supported
  -f BENCHMARKFOLDER, --folder BENCHMARKFOLDER
                        Folder in which all benchmark executables lie
  -nc, --no-confirmation
                        If this flag is set then the script will not ask if it should run the found benchmarks
  -m RUNMESSAGE, --message RUNMESSAGE
                        If a message is present, then this will be written into the log file. This may help if one executes different
                        benchmarks
  -d, --dry-run         If this flag is set, then the benchmarks will not be executed, so a dry run is performed
  -r RESULTFOLDER, --result-folder RESULTFOLDER
                        Folder in which all benchmark results shall be written. If not set then a new folder will be created
  -jp JUSTPLOT, --just-plot JUSTPLOT
                        CSV file that will be used as data for plotting
  -jpf JUSTPLOTFILE, --just-plot-file JUSTPLOTFILE
                        Python file that will be used for plotting
  -jrb JUSTRUNBENCHMARK, --just-run-benchmark JUSTRUNBENCHMARK
                        Execute the benchmark binaries that generate the CSV files
  -ba BENCHMARKWITHARGS [BENCHMARKWITHARGS ...], --benchmarks-with-args BENCHMARKWITHARGS [BENCHMARKWITHARGS ...]
                        Expects a list of [["name of binary", "args"], ["name of binary", "args"], ...]
