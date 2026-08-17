# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# The stages of the systest pipeline, in the order they run:
# configure, discover test files, read one, parse it, rewrite it to SQL, run it and check the results.
add_subdirectory(Config)
add_subdirectory(Discovery)
add_subdirectory(Parser)
add_subdirectory(Rewriter)
add_subdirectory(Runner)
add_subdirectory(ResultChecker)

add_source_files(nes-systest-lib
        Benchmark.cpp
        Executor.cpp
        Logging.cpp
        Progress.cpp
        TestFilePartition.cpp
        WorkingDirectoryGuard.cpp
)
