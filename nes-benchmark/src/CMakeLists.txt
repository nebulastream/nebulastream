# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_subdirectory(DataGeneration)

add_subdirectory(MicroBenchmarks)

add_source_files(nes-benchmark
        E2E/Configurations/E2EBenchmarkConfig.cpp
        E2E/Configurations/E2EBenchmarkConfigOverAllRuns.cpp
        E2E/Configurations/E2EBenchmarkConfigPerRun.cpp
        DataProvider/DataProvider.cpp
        DataProvider/ExternalProvider.cpp
        DataProvider/InternalProvider.cpp
        IngestionRateGeneration/CustomIngestionRateGenerator.cpp
        IngestionRateGeneration/IngestionRateGenerator.cpp
        IngestionRateGeneration/TrigonometricIngestionRateGenerator.cpp
        IngestionRateGeneration/UniformIngestionRateGenerator.cpp
        Measurements.cpp
        E2E/E2ERunner.cpp
        E2E/E2ESingleRun.cpp)

