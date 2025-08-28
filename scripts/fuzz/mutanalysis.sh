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

export CCACHE_DIR=/ccache

if [ ! -d /out ]
then
    echo expects /out dir to exist
    exit 1
fi

if [ -z "$FWC_NES_CORPORA_TOKEN" ]
then
    echo pls set FWC_NES_CORPORA_TOKEN
    exit 1
fi

out_dir=/out/mutanalysis_$(date -u +"%Y-%m-%dT%H-%M-%S")
mkdir $out_dir
mkdir $out_dir/crashes

out_log=$out_dir/log.txt
log_out() {
    echo "$*" | tee -a $out_log
}

git clone --depth=1 --branch=fuzz https://github.com/nebulastream/nebulastream.git
git clone --depth=1 "https://fwc:$FWC_NES_CORPORA_TOKEN@github.com/fwc/nes-corpora.git" /nes-corpora
cd nebulastream || exit 1

python3 standalone_mutator.py -o mutations $(git ls-files -- "*.cpp" | grep -v test | grep -v fuzz)

export AFL_SKIP_CPUFREQ=1
export AFL_I_DONT_CARE_ABOUT_MISSING_CRASHES=1

cmake -B cmake-build-nrm -DCMAKE_BUILD_TYPE=RelWithDebug -DUSE_LIBFUZZER=ON -DCMAKE_CXX_SCAN_FOR_MODULES=OFF
CC=afl-clang-lto CXX=afl-clang-lto++ cmake -B cmake-build-afl -DCMAKE_BUILD_TYPE=RelWithDebug -DUSE_LIBFUZZER=ON -DCMAKE_CXX_SCAN_FOR_MODULES=OFF
CC=hfuzz-clang   CXX=hfuzz-clang++   cmake -B cmake-build-hfz -DCMAKE_BUILD_TYPE=RelWithDebug -DUSE_LIBFUZZER=ON -DCMAKE_CXX_SCAN_FOR_MODULES=OFF

cmake --build cmake-build-nrm
cmake --build cmake-build-afl
cmake --build cmake-build-hfz

for patch in $(find mutations -name "*.patch" -print0 | xargs -0 sha256sum | sort | awk '{ print $2 }')
do
    if ! python3 check_mutations.py $patch cmake-build-nrm/compile_commands.json || ! cmake --build cmake-build-nrm
    then
        log_out cannot build $patch
        continue
    fi

    if ! ctest -j 32 --test-dir cmake-build-nrm --stop-on-failure --quiet
    then
        log_out mutant $patch killed by ctest
    fi


    KILLED_BY_CORPUS=false
    for harness in snw-proto-fuzz snw-strict-fuzz sql-parser-simple-fuzz
    do
        if ! $(find cmake-build-nrm -name $harness) -runs=0 /nes-corpora/$harness/corpus > /dev/null 2> /dev/null
        then
            log_out mutant $patch killed by $harness with corpus
            KILLED_BY_CORPUS=true
        fi
    done

    if $KILLED_BY_CORPUS
    then
        continue
    fi

    AFL_CORPUS_LOADED=false
    for harness in snw-proto-fuzz snw-strict-fuzz sql-parser-simple-fuzz
    do
        timeout 10m $(find cmake-build-nrm -name $harness) -jobs=$(( $(nproc) / 2 )) /nes-corpora/$harness/corpus &

        while [ $(pgrep -a $harness | wc -l) = $(( $(nproc) / 2 + 1 )) ]
        do
            sleep 1
        done

        # kill all child processes
        pkill -P $$

        for crash in crash-*
        do
            mv $crash $out_dir/crashes
            log_out mutant $patch killed by libfuzzer $harness with $crash
        done


        if ! $AFL_CORPUS_LOADED
        then
            afl-fuzz -i ~/nes-corpora/$harness/corpus -o afl-base-out -t 10000 -I "touch crash_found" -M main  -- $(find cmake-build-afl -name $harness) > afl-main.log &
            for i in $(seq $(( $(nproc) / 2 - 1 )) )
            do
            afl-fuzz -i ~/nes-corpora/$harness/corpus -o afl-base-out -t 10000 -I "touch crash_found" -S sub-$i -- $(find cmake-build-afl -name $harness) > afl-sub-$1.log &
            done
            wait
            AFL_CORPUS_LOADED=true
        fi
        rm -r afl-out
        cp -r afl-base-out afl-out

        timeout 10m afl-fuzz -i - -o afl-out -t 10000 -I "touch crash_found" -M main  -- $(find cmake-build-afl -name $harness) > afl-main.log &
        for i in $(seq $(( $(nproc) / 2 - 1 )) )
        do
        timeout 10m afl-fuzz -i - -o afl-out -t 10000 -I "touch crash_found" -S sub-$i -- $(find cmake-build-afl -name $harness) > afl-sub-$1.log &
        done

        while pgrep -a $harness
        do
            if [ -e crash_found ]
            then
                # kill all child processes
                pkill -P $$
                rm crash_found

                for crash in afl-out/main/crashes/*
                do
                    mv $crash $out_dir/crashes
                    log_out mutant $patch killed by aflpp $harness with $crash
                done

                for i in $(seq $(( $(nproc) / 2 - 1 )) )
                do
                    for crash in afl-out/sub-$i/crashes/*
                    do
                        mv $crash $out_dir/crashes
                        log_out mutant $patch killed by aflpp $harness with $crash
                    done
                done
            fi
        done

        if ! honggfuzz --run_time 600 -i /nes-corpora/$harness/corpus -o hfz_out --crash-dir hf-crashes --exit_upon_crash --exit_code_upon_crash 1 -- $(find cmake-build-hfz -name $harness) 2> hf.log
        then
            log_out mutant $patch killed by honggfuzz $harness with $crash
        fi

        for crash in hf-crashes/*
        do
            mv "$crash" $out_dir/crashes
            log_out mutant $patch killed by honggfuzz $harness with "$crash"
        done
    done
done
