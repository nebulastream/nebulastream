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

#include <Runtime/BufferManager.hpp>
#include <Benchmarking/MicroBenchmarkASPUtil.hpp>
#include <Benchmarking/MicroBenchmarkRun.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Timer.hpp>
#include <Util/yaml/Yaml.hpp>

namespace NES::ASP::Benchmarking {

void MicroBenchmarkRun::run() {

    // Creating bufferManager for this run
    auto bufferManager = std::make_shared<Runtime::BufferManager>(bufferSize, numberOfBuffers);

    NES_INFO("Creating input buffer and accuracy buffer...");
    auto allRecordBuffers = this->createInputRecords(bufferManager);
    auto allAccuracyBuffers = this->createAccuracyRecords(bufferManager);

    auto synopsis = AbstractSynopsis::create(synopsesArguments);
    auto memoryProvider = ASP::Util::createMemoryProvider(bufferSize, schema);


    Timer timer("benchmarkLoop");
    for (auto rep = 0UL; rep < reps; ++rep) {
        // Creating strings here for the snapshots
        const std::string addToSynopsisSnapshotName = "addToSynopsis_loopIteration_" + std::to_string(rep + 1);
        const std::string getApproximateSnapshotName = "getApproximate_loopIteration_" + std::to_string(rep + 1);

        // Starting the timer again
        timer.start();

        // For now, we do not have to take care of windowing. This is fixed in issue #3628
        synopsis->initialize();

        for (auto& buffer : allRecordBuffers) {
            auto numberOfRecords = buffer.getNumRecords();
            auto bufferAddress = buffer.getBuffer();
            for (Nautilus::Value<Nautilus::UInt64> i = (uint64_t) 0; i < numberOfRecords; i = i + (uint64_t) 1) {
                auto record = memoryProvider->read({}, bufferAddress, i);
                synopsis->addToSynopsis(record);
            }
        }

        timer.snapshot(addToSynopsisSnapshotName);
        auto allApproximateBuffers = synopsis->getApproximate(bufferManager);

        timer.snapshot(getApproximateSnapshotName);
        timer.pause();
        auto duration = timer.getRuntimeFromSnapshot(timer.createFullyQualifiedSnapShotName(addToSynopsisSnapshotName)) +
                        timer.getRuntimeFromSnapshot(timer.createFullyQualifiedSnapShotName(getApproximateSnapshotName));


        // Checking for the accuracy and calculating the throughput of this loop
        auto accuracy = this->compareAccuracy(allAccuracyBuffers, allApproximateBuffers);

        auto throughputInTuples = allRecordBuffers.size() / (double)(duration / NANO_TO_SECONDS_MULTIPLIER);
        microBenchmarkResult.emplace_back(throughputInTuples, accuracy);
    }

    NES_INFO("Loop timers: " << timer);
}

std::vector<MicroBenchmarkRun> MicroBenchmarkRun::parseMicroBenchmarksFromYamlFile(const std::string& yamlConfigFile) {
    std::vector<MicroBenchmarkRun> retVector;

    Yaml::Node configFile;
    Yaml::Parse(configFile, yamlConfigFile.c_str());
    if (configFile.IsNone()) {
        NES_THROW_RUNTIME_ERROR("Could not parse " << yamlConfigFile << "!");
    }

    // Parsing all required members from the yaml file
    auto parsedReps = ASP::Util::parseReps(configFile["reps"]);
    auto parsedSynopsisArguments = ASP::Util::parseSynopsisArguments(configFile["synopsis"]);
    auto parsedAggregations = ASP::Util::parseAggregations(configFile["aggregation"]);
    auto parsedWindowSizes = ASP::Util::parseWindowSizes(configFile["windowSize"]);
    auto parsedBufferSizes = ASP::Util::parseBufferSizes(configFile["bufferSize"]);
    auto parsedNumberOfBuffers = ASP::Util::parseNumberOfBuffers(configFile["numberOfBuffers"]);



    for (auto& synopsisArgument : parsedSynopsisArguments) {
        for (auto& aggregation : parsedAggregations) {
            for (auto& windowSize : parsedWindowSizes) {
                for (auto& bufferSize : parsedBufferSizes) {
                    for (auto& numberOfBuffers : parsedNumberOfBuffers) {
                        retVector.emplace_back(synopsisArgument, aggregation.schema, bufferSize, numberOfBuffers,
                                               windowSize, reps, aggregation.inputFile, aggregation.accuracyFile);
                    }
                }
            }
        }
    }

    return retVector;
}
MicroBenchmarkRun::MicroBenchmarkRun(const SynopsisArguments& synopsesArguments,
                                     const SchemaPtr& schema,
                                     const uint32_t bufferSize,
                                     const uint32_t numberOfBuffers,
                                     const size_t windowSize,
                                     const size_t reps,
                                     const std::string& inputFile,
                                     const std::string& accuracyFile)
    : synopsesArguments(synopsesArguments), schema(schema), bufferSize(bufferSize), numberOfBuffers(numberOfBuffers),
      windowSize(windowSize), reps(reps), inputFile(inputFile), accuracyFile(accuracyFile) {}

} // namespace NES::ASP::Benchmarking