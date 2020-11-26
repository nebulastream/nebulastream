#! /bin/bash

function is_installed ()
{
	command -v "$1" >/dev/null 2>&1 || { echo >&2 ">>$1<< is required but not installed. Aborting."; exit 1; }
}


usage="$(basename "$0") [-h] [-p <process id>] [-l <flamegraph git location>] [-d <record duration in seconds] [-g <class or function name>] [-f   -- This script will create flamegraphs via arguments

Arguments:
	-h  show this help text
	-p  id of process that will be 
	-d  duration of perf record, default will be 10 seconds
	-l  location of flamegraph git, if not set then flamegraph git will be downloaded
	-g  flamegraph will be created for this function/class name
	-n  name of output svg"


LOCATION_FLAMEGIT="/tmp";
RECORD_DURATION=10;
PROCESS_ID=-12;
GREP_NAME="NES";
PERF_FREQ=5000;
NAME_SVG="output"

while getopts 'p:d:l:g:f:h' option
do
	case "${option}" in
		p) 	PROCESS_ID=${OPTARG};;
		d) 	RECORD_DURATION=${OPTARG};;
		l) 	LOCATION_FLAMEGIT="${OPTARG}";;
		g) 	GREP_NAME="${OPTARG}";;
		f)	PERF_FREQ=${OPTARG};;
		h) 	echo "$usage"
			exit 1
			;;
		\?) echo "$usage" >&2
			exit 1
			;;
	esac;
done;

is_installed "perf";
is_installed "git";

echo "Arguments after parsing:
- LOCATION_FLAMEGIT=$LOCATION_FLAMEGIT
- RECORD_DURATION=$RECORD_DURATION
- PROCESS_ID=$PROCESS_ID
- GREP_NAME=$GREP_NAME
- PERF_FREQ=$PERF_FREQ
- NAME_SVG=$NAME_SVG.svg"


read -p "Process $PROCESS_ID will be recorded for $RECORD_DURATION seconds with a frequency of $PERF_FREQ Hz. Continue? [Y/n] " choice
case "$choice" in 
  n|N ) echo "Exiting now..." && exit 1;;
  * ) echo -e "Starting recording process "$PROCESS_ID"...";;
esac

sudo perf record -F $PERF_FREQ -p $PROCESS_ID -g -- sleep $RECORD_DURATION
sudo chown $USER:$USER perf.data
perf script > out.perf
echo -e "Done recording!\n"


if [ ! -d "$LOCATION_FLAMEGIT/FlameGraph" ] 
then
	echo -e "\nCould not find flamegraph git. Will clone flamegraph git to /tmp ..."
	git clone --progress git@github.com:brendangregg/FlameGraph.git /tmp/FlameGraph
	echo -e "Done cloning!\n"
	LOCATION_FLAMEGIT="/tmp";	
fi


echo -e "Starting collapsing stacks..."
$LOCATION_FLAMEGIT/FlameGraph/stackcollapse-perf.pl out.perf > out.folded
echo "Done collapsing!\n"

grep $GREP_NAME out.folded | $LOCATION_FLAMEGIT/FlameGraph/flamegraph.pl > $NAME_SVG.svg
echo -e "Created flamegraph $NAME_SVG.svg in $PWD!"































