#! /bin/bash

function is_installed ()
{
	command -v "$1" >/dev/null 2>&1 || { echo >&2 ">>$1<< is required but not installed. Aborting."; exit 1; }
}

function resolve_flamegraphgit ()
{
	if [ ! -d "$LOCATION_FLAMEGIT/FlameGraph" ]
	then
		echo -e "\nCould not find flamegraph git. Will clone flamegraph git to /tmp ..."
		git clone --progress git@github.com:brendangregg/FlameGraph.git /tmp/FlameGraph
		echo -e "Done cloning!\n"
		LOCATION_FLAMEGIT="/tmp";
	fi
}

function plot_flamegraph()
{
	grep $1 out.folded | $2/FlameGraph/flamegraph.pl > $3.svg
	echo -e "Created flamegraph $3.svg in $PWD!"
}


usage="$(basename "$0") [-h] [-p <process id>] [-l <flamegraph git location>] [-d <record duration in seconds] [-g <class or function name>] [-f <freq>] [-n <name output svg>]

-- This script will create flamegraphs via arguments:
	-h  show this help text
	-p  id of process that will be recorded
	-d  duration of perf record
	-l  location of flamegraph git, if not set then flamegraph git will be downloaded
	-g  flamegraph will be created for this function/class name
	-n  name of output svg
	-s  if set, will skip the recording and will just plot a flamegraph. Excepts fodled stack in file \"out.folded\"!
	"


LOCATION_FLAMEGIT="/tmp";
RECORD_DURATION=10;
PROCESS_ID=-12;
GREP_NAME="NES";
PERF_FREQ=5000;
NAME_SVG="output";
SKIP_RCD="false";

while getopts 'p:d:l:g:f:n:sh' option
do
	case "${option}" in
		p) 	PROCESS_ID=${OPTARG};;
		d) 	RECORD_DURATION=${OPTARG};;
		l) 	LOCATION_FLAMEGIT="${OPTARG}";;
		g) 	GREP_NAME="${OPTARG}";;
		f)	PERF_FREQ=${OPTARG};;
		n)	NAME_SVG=${OPTARG};;
		s)	SKIP_RCD="true";;
		h) 	echo "$usage"
			exit 
			;;
		\?) echo -e "$usage" >&2
			exit 1
			;;
	esac;
done;

is_installed "perf";
is_installed "git";

echo -e "Arguments after parsing:
- LOCATION_FLAMEGIT=$LOCATION_FLAMEGIT
- RECORD_DURATION=$RECORD_DURATION
- PROCESS_ID=$PROCESS_ID
- GREP_NAME=$GREP_NAME
- PERF_FREQ=$PERF_FREQ
- NAME_SVG=$NAME_SVG.svg\n"


if [ "true" == "$SKIP_RCD" ]; then
    echo -e "Will skip recording and just plot flamegraph..."

	resolve_flamegraphgit
	plot_flamegraph $GREP_NAME $LOCATION_FLAMEGIT $NAME_SVG
	
	exit 0
fi

read -p "Process $PROCESS_ID will be recorded for $RECORD_DURATION seconds with a frequency of $PERF_FREQ Hz. Continue? [Y/n] " choice
case "$choice" in 
  n|N ) echo -e "Exiting now..." && exit 1;;
  * ) echo -e "Starting recording process "$PROCESS_ID"...";;
esac

sudo perf record -F $PERF_FREQ -p $PROCESS_ID -g -- sleep $RECORD_DURATION
sudo chown $USER:$USER perf.data
perf script > out.perf
echo -e "Done recording!\n"


resolve_flamegraphgit;

echo -e "Starting collapsing stacks..."
$LOCATION_FLAMEGIT/FlameGraph/stackcollapse-perf.pl out.perf > out.folded
echo -e "Done collapsing!\n"

plot_flamegraph $GREP_NAME $LOCATION_FLAMEGIT $NAME_SVG;


























