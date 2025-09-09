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

if [ ! -d /out ] || [ ! -d /wdir ] || [ ! -d /ccache ]
then
    echo expects /out, /wdir, and /ccache to exist
    exit 1
fi

if [ -z "$FWC_NES_CORPORA_TOKEN" ]
then
    echo pls set FWC_NES_CORPORA_TOKEN
    exit 1
fi

if [ $(ulimit -c) != 0 ]
then
    echo "better use docker flag '--ulimit core=0'"
    exit 1
fi

git status > /dev/null
if ! git diff-files --quiet
then
    echo requires clean working tree
    exit 1
fi

out_dir=/out/mutanalysis_$(date -u +"%Y-%m-%dT%H-%M-%S")
mkdir $out_dir
mkdir $out_dir/crashes

out_log=$out_dir/log.txt
log_out() {
    echo "$(date -Is) $*" | tee -a $out_log
}

wdir=/wdir/mutanalysis_$(date -u +"%Y-%m-%dT%H-%M-%S")

mkdir $wdir
cd $wdir

git clone --depth=1 --branch=fuzz https://github.com/nebulastream/nebulastream.git
git clone --depth=1 "https://fwc:$FWC_NES_CORPORA_TOKEN@github.com/fwc/nes-corpora.git" /nes-corpora
cd nebulastream || exit 1

rm -rf mutations
python3 standalone_mutator.py -o mutations $(git ls-files -- "*.cpp" | grep -v test | grep -v fuzz)

export AFL_SKIP_CPUFREQ=1
export AFL_I_DONT_CARE_ABOUT_MISSING_CRASHES=1

                                     cmake -B cmake-build-nrm -DCMAKE_BUILD_TYPE=RelWithDebug -DCMAKE_CXX_SCAN_FOR_MODULES=OFF
                                     cmake -B cmake-build-lfz -DCMAKE_BUILD_TYPE=RelWithDebug -DCMAKE_CXX_SCAN_FOR_MODULES=OFF -DUSE_LIBFUZZER=ON
# CC=afl-clang-lto CXX=afl-clang-lto++ cmake -B cmake-build-afl -DCMAKE_BUILD_TYPE=RelWithDebug -DCMAKE_CXX_SCAN_FOR_MODULES=OFF -DUSE_LIBFUZZER=ON
CC=hfuzz-clang   CXX=hfuzz-clang++   cmake -B cmake-build-hfz -DCMAKE_BUILD_TYPE=RelWithDebug -DCMAKE_CXX_SCAN_FOR_MODULES=OFF -DUSE_LIBFUZZER=ON

cmake --build cmake-build-nrm
cmake --build cmake-build-lfz --target snw-proto-fuzz snw-strict-fuzz snw-text-fuzz sql-parser-simple-fuzz
# cmake --build cmake-build-afl --target snw-proto-fuzz snw-strict-fuzz snw-text-fuzz sql-parser-simple-fuzz
cmake --build cmake-build-hfz --target snw-proto-fuzz snw-strict-fuzz snw-text-fuzz sql-parser-simple-fuzz

for patch in $(find mutations -name "*.patch" -print0 | xargs -0 -I {} sh -c 'echo $(tail -8 {} | sha256sum) {}' | sort | awk '{ print $3 }')
do
    git status > /dev/null
    if ! git diff-files --quiet
    then
        log_out requires clean working tree
        exit 1
    fi

    log_out checking $patch
    if ! python3 check_mutations.py $patch cmake-build-nrm/compile_commands.json
    then
        log_out cannot build $patch quickly
        continue
    fi

    cmake --build cmake-build-nrm --target clean
    cmake --build cmake-build-lfz --target clean
    cmake --build cmake-build-hfz --target clean

    patched_file=$(head -n 1 $patch | cut -d" " -f 2)
    cp $patched_file $patched_file.bak
    if ! patch --forward $patched_file < $patch
    then
        log_out cannot apply $patch
        mv $patched_file.bak $patched_file
        continue
    fi

    if ! cmake --build cmake-build-nrm -j $(nproc) > /dev/null 2>/dev/null
    then
        log_out cannot build $patch
        mv $patched_file.bak $patched_file
        continue
    fi

    log_out running ctest
    if ! timeout --kill-after=1m 30m ctest -j 32 --test-dir cmake-build-nrm --stop-on-failure --repeat until-pass:3 --quiet -O ctest.log
    then
        for testcase in $(cat ctest.log | grep Failed | awk '$2 == "-" { print $3 }')
        do
            log_out mutant $patch killed by ctest with $testcase
        done
    fi


    log_out running corpus
    KILLED_BY_CORPUS=false
    for harness in snw-proto-fuzz snw-strict-fuzz sql-parser-simple-fuzz
    do
        if ! $(find cmake-build-lfz -name $harness) -runs=0 /nes-corpora/$harness/corpus > /dev/null 2> /dev/null
        then
            for crash in crash-*
            do
                log_out mutant $patch killed by $harness corpus with $crash
            done
            log_out mutant $patch killed by $harness with corpus
            KILLED_BY_CORPUS=true
        fi
    done

    if $KILLED_BY_CORPUS
    then
        mv $patched_file.bak $patched_file
        continue
    fi

    for harness in snw-proto-fuzz snw-strict-fuzz sql-parser-simple-fuzz
    do
        log_out running $harness libfuzzer
        $(find cmake-build-lfz -name $harness) -jobs=$(( $(nproc) / 2 )) /nes-corpora/$harness/corpus &

        ( sleep 600; touch timed_out ) &

        while pgrep -P $$ > /dev/null
        do
            for crash in crash-*
            do
                # kill all child processes
                pkill -P $$

                mv $crash $out_dir/crashes
                log_out mutant $patch killed by libfuzzer $harness with $crash

                sleep 10

                while pgrep -P $$ > /dev/null
                do
                    echo some children still alive
                    pgrep -P $$ || true
                    echo sending sigkill
                    pkill -P $$ --signal kill || true
                    sleep 1
                done
            done

            if [ -e timed_out ]
            then
                pkill -P $$
            fi
            sleep 1
        done
        rm -f timed_out


        # log_out running $harness aflpp
        #
        # rm -rf afl-out
        #
        # afl-fuzz -V 600 -i /nes-corpora/$harness/corpus -o afl-out -t 10000 -I "touch crash_found" -M main  -- $(find cmake-build-afl -name $harness) > afl-main.log &
        # for i in $(seq $(( $(nproc) / 2 - 1 )) )
        # do
        # afl-fuzz -V 600 -i /nes-corpora/$harness/corpus -o afl-out -t 10000 -I "touch crash_found" -S sub-$i -- $(find cmake-build-afl -name $harness) > afl-sub-$i.log &
        # done
        #
        # while pgrep -P $$ > /dev/null
        # do
        #     if [ -e crash_found ]
        #     then
        #         # kill all child processes
        #         pkill -P $$
        #         rm crash_found
        #
        #         for crash in afl-out/main/crashes/id*
        #         do
        #             mv $crash $out_dir/crashes
        #             log_out mutant $patch killed by aflpp $harness with $crash
        #         done
        #
        #         for i in $(seq $(( $(nproc) / 2 - 1 )) )
        #         do
        #             for crash in afl-out/sub-$i/crashes/id*
        #             do
        #                 mv $crash $out_dir/crashes
        #                 log_out mutant $patch killed by aflpp $harness with $crash
        #             done
        #         done
        #
        #         sleep 10
        #
        #         while pgrep -P $$ > /dev/null
        #         do
        #             echo some children still alive
        #             pgrep -P $$ || true
        #             echo sending sigkill
        #             pkill -P $$ --signal kill || true
        #             sleep 1
        #         done
        #     fi
        #     sleep 1
        # done

        log_out running $harness honggfuzz
        if ! honggfuzz --run_time 600 -i /nes-corpora/$harness/corpus -o hfz_out --crashdir hf-crashes --exit_upon_crash --exit_code_upon_crash 1 -- $(find cmake-build-hfz -name $harness) 2> hf.log
        then
            true
        fi

        for crash in hf-crashes/*
        do
            if $(find cmake-build-lfz -name $harness) $crash > /dev/null 2> /dev/null
            then
                log_out mutant $patch crash $crash seems to be a fluke
                rm $crash
                continue
            fi
            mv "$crash" $out_dir/crashes
            log_out mutant $patch killed by honggfuzz $harness with "$crash"
        done

        rm -rf hf-crashes/
    done
    log_out mutant $patch survived
    mv $patched_file.bak $patched_file
done
