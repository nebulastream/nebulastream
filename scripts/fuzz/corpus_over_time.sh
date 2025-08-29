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

set -e
set -u
shopt -s nullglob

export CCACHE_DIR=/ccache

if [ ! -d /out ] || [ ! -d /ccache ]
then
    echo expects /out and /ccache to exist
    exit 1
fi

if [ $# != 3 ]
then
    echo usage: $0 engine harness duration
    exit 1
fi

engine=$1
harness=$2
duration=$3

if [ $harness != "sql-parser-simple-fuzz" ] && [ $harness != "snw-text-fuzz" ] && [ $harness != "snw-proto-fuzz" ] && [ $harness != "snw-strict-fuzz" ]
then
    echo unknown harness $harness, expexting one of
    echo
    echo sql-parser-simple-fuzz
    echo snw-text-fuzz
    echo snw-proto-fuzz
    echo snw-strict-fuzz
    exit 1
fi

if [ $harness == "sql-parser-simple-fuzz" ] && [ $harness == "snw-text-fuzz" ]
then
    input_type=nesql
else
    input_type=txtpb
fi

mkdir /corpus-nesql
mkdir /corpus-txtpb

git clone --depth=1 --branch=fuzz https://github.com/nebulastream/nebulastream.git /nebulastream
cd /nebulastream || exit 1

if [ $harness != "snw-strict-fuzz" ]
then
    cmake -B systest-build -DCMAKE_BUILD_TYPE=RelWithDebug -DUSE_LIBFUZZER=ON -DCMAKE_CXX_SCAN_FOR_MODULES=OFF
    cmake --build systest-build --target systest

    mkdir queries
    cd queries
    ../systest-build/nes-systests/systest/systest --dump-queries

    mv ./*.nesql /corpus-nesql
    mv ./*.txtpb /corpus-txtpb

    cd ..
else
    echo foo > /corpus-txtpb/foo
fi


if [ $engine = "libfuzzer" ]
then
    true
elif [ $engine = "aflpp" ]
then
    export AFL_SKIP_CPUFREQ=1
    export AFL_I_DONT_CARE_ABOUT_MISSING_CRASHES=1

    export CC=afl-clang-lto
    export CXX=afl-clang-lto++
elif [ $engine = "honggfuzz" ]
then
    export CC=hfuzz-clang
    export CXX=hfuzz-clang++
else
    echo unknown engine $engine. Must be one of libfuzzer, aflpp, or honggfuzz
    exit 1
fi

cmake -B build -DCMAKE_BUILD_TYPE=RelWithDebug -DUSE_LIBFUZZER=ON -DCMAKE_CXX_SCAN_FOR_MODULES=OFF
cmake --build build --target $harness
absolute_harness=$(pwd)/$(find build -name $harness)

# must not have colon in workdir: https://github.com/google/honggfuzz/issues/530
work_dir=/out/$harness"_"$engine"_"$(date -u +"%Y-%m-%dT%H-%M-%S")
mkdir $work_dir
cd $work_dir

git init . -b main
git config user.name "peter"
git config user.email "peter@enternet.de"

while true
do
    git add .
    git commit -m foo || true
    sleep 60
done &

if [ $engine = "libfuzzer" ]
then
    mkdir corpus
    timeout $duration $absolute_harness -jobs=100000 -workers=1 $work_dir/corpus /corpus-$input_type
elif [ $engine = "aflpp" ]
then
    timeout $duration afl-fuzz -t 10000    -i /corpus-$input_type -o $work_dir/corpus -- $absolute_harness
elif [ $engine = "honggfuzz" ]
then
    timeout $duration honggfuzz -t 10 -n 1 -i /corpus-$input_type -o $work_dir/corpus -- $absolute_harness
else
    echo wat
    exit 1
fi

kill %1

cd /nebulastream

cmake -B cov-build -DCMAKE_BUILD_TYPE=Debug -DUSE_LIBFUZZER=ON -DCMAKE_CXX_SCAN_FOR_MODULES=OFF -DUSE_GCOV=ON
cmake --build cov-build --target $harness

apt install -y gcovr

fuzzer=$(pwd)/$(find cov-build -name $harness)

mkdir $work_dir/log
mkdir $work_dir/cov

if [ $engine = "alfpp" ]
then
    crash_dir=$work_dir/corpus/default/queue
else
    crash_dir=$work_dir/corpus
fi


i=0
for commit in $(git -C $work_dir rev-list main -- $crash_dir | tac)
do
    fuzzer_ran=false
    pi=$(printf '%04d' $i)
    for file in $(git -C $work_dir ls-tree --name-only -r $commit -- $crash_dir)
    do
        $fuzzer $work_dir/$file > $work_dir/log/cov_fuzz_$pi.log 2>&1 || true
        fuzzer_ran=true 
    done

    if $fuzzer_ran
    then
        gcovr --gcov-executable="llvm-cov-19 gcov" \
        --json         $work_dir/cov/gcovr_all_$pi-$commit.json \
        --json-summary $work_dir/cov/gcovr_sum_$pi-$commit.json
    fi
    i=$(( i + 1 ))
done
