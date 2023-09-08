/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <E2E/E2ERunner.hpp>
#include <fstream>

namespace NES::Benchmark {

E2EBenchmarkConfig parseYamlConfig(std::string configPath, std::string logPath) {
    NES::Logger::setupLogging(logPath, E2EBenchmarkConfig::getLogLevel(configPath));
    E2EBenchmarkConfig e2EBenchmarkConfig;

    try {
        e2EBenchmarkConfig = E2EBenchmarkConfig::createBenchmarks(configPath);
        NES_INFO("E2ERunner: Created the following experiments: {}", e2EBenchmarkConfig.toString());
    } catch (std::exception& e) {
        NES_THROW_RUNTIME_ERROR("E2ERunner: Error while creating benchmarks!");
    }

    return e2EBenchmarkConfig;
}

void executeSingleRun(E2EBenchmarkConfigPerRun& configPerRun,
                      E2EBenchmarkConfigOverAllRuns& configOverallRuns,
                      int rpcPort,
                      int restPort) {
    E2ESingleRun singleRun(configPerRun, configOverallRuns, rpcPort, restPort);
    singleRun.run();
    NES_INFO("Done with single experiment!");
}

void writeHeaderToCsvFile(E2EBenchmarkConfigOverAllRuns& configOverAllRuns) {
    std::ofstream ofs;
    ofs.open(configOverAllRuns.outputFile->getValue(), std::ofstream::out | std::ofstream::app);
    ofs << "BenchmarkName,NES_VERSION,nautilusBackend,SchemaSize,"
           "stageBufferSize,"
           "timestamp,processedTasks,processedBuffers,processedTuples,latencySum,"
           "queueSizeSum,availGlobalBufferSum,availFixedBufferSum,"
           "tuplesPerSecond,tasksPerSecond,bufferPerSecond,mebiBPerSecond,"
           "numberOfWorkerOfThreads,numberOfDeployedQueries,numberOfSources,bufferSizeInBytes,inputType,dataProviderMode,"
           "queryString"
        << std::endl;
    ofs.close();
}
}// namespace NES::Benchmark