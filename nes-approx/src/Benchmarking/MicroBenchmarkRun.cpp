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

#include <Benchmarking/MicroBenchmarkASPUtil.hpp>
#include <Benchmarking/MicroBenchmarkRun.hpp>
#include <Runtime/BufferManager.hpp>
#include <Sources/Parsers/CSVParser.hpp>
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
    auto memoryProvider = ASP::Util::createMemoryProvider(bufferSize, aggregation.inputSchema);
    synopsis->setAggregationFunction(aggregation.createAggregationFunction());
    synopsis->setFieldNameAggregation(aggregation.fieldNameAggregation);
    synopsis->setFieldNameApproximate(aggregation.fieldNameApproximate);
    synopsis->setOutputSchema(aggregation.outputSchema);

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


    retVector.reserve(parsedSynopsisArguments.size() * parsedAggregations.size() *
                      parsedWindowSizes.size() * parsedBufferSizes.size() *
                      parsedNumberOfBuffers.size());
    auto cnt = 0UL;
    for (auto& synopsisArgument : parsedSynopsisArguments) {
        for (auto& aggregation : parsedAggregations) {
            for (auto& windowSize : parsedWindowSizes) {
                for (auto& bufferSize : parsedBufferSizes) {
                    for (auto& numberOfBuffers : parsedNumberOfBuffers) {
                        retVector[++cnt] = MicroBenchmarkRun(synopsisArgument, aggregation, bufferSize, numberOfBuffers,
                                                             windowSize, parsedReps);
                    }
                }
            }
        }
    }

    return retVector;
}

MicroBenchmarkRun::MicroBenchmarkRun(const SynopsisArguments& synopsesArguments,
                                     const YamlAggregation& aggregation,
                                     const uint32_t bufferSize,
                                     const uint32_t numberOfBuffers,
                                     const size_t windowSize,
                                     const size_t reps)
    : synopsesArguments(synopsesArguments), aggregation(aggregation), bufferSize(bufferSize),
      numberOfBuffers(numberOfBuffers), windowSize(windowSize), reps(reps){}

MicroBenchmarkRun& MicroBenchmarkRun::operator=(const MicroBenchmarkRun& other) {
    // If this is the same object, then return it
    if (this == &other) {
        return *this;
    }

    // Otherwise create a new object
    synopsesArguments = other.synopsesArguments;
    aggregation = other.aggregation;
    bufferSize = other.bufferSize;
    numberOfBuffers = other.numberOfBuffers;
    windowSize = other.windowSize;
    reps = other.reps;
    microBenchmarkResult = std::vector(other.microBenchmarkResult);

    return *this;
}


MicroBenchmarkRun::MicroBenchmarkRun(const MicroBenchmarkRun& other) {
    synopsesArguments = other.synopsesArguments;
    aggregation = other.aggregation;
    bufferSize = other.bufferSize;
    numberOfBuffers = other.numberOfBuffers;
    windowSize = other.windowSize;
    reps = other.reps;
    microBenchmarkResult = std::vector(other.microBenchmarkResult);
}

std::string MicroBenchmarkRun::getHeaderAsCsv() {
    std::stringstream stringStream;
    stringStream << synopsesArguments.getHeaderAsCsv()
                 << "," << aggregation.getHeaderAsCsv()
                 << ",outputSchema"
                 << ",bufferSize"
                 << ",numberOfBuffers"
                 << ",windowSize"
                 << ",reps"
                 << "," << microBenchmarkResult[0].getHeaderAsCsv();

    return stringStream.str();
}

std::string MicroBenchmarkRun::getRowsAsCsv() {
    std::stringstream stringStream;

    for (auto& benchmarkResult : microBenchmarkResult) {
        stringStream << synopsesArguments.getValuesAsCsv()
                     << "," << aggregation.getValuesAsCsv()
                     << "," << bufferSize
                     << "," << numberOfBuffers
                     << "," << windowSize
                     << "," << reps
                     << "," << benchmarkResult.getRowAsCsv()
                     << std::endl;
    }
    return stringStream.str();
}

std::string MicroBenchmarkRun::toString() {
    std::stringstream stringStream;
    stringStream << std::endl << " - synopsis arguments: " << synopsesArguments.toString()
                 << std::endl << " - aggregation:" << aggregation.toString()
                 << std::endl << " - bufferSize:" << bufferSize
                 << std::endl << " - numberOfBuffers:" << numberOfBuffers
                 << std::endl << " - windowSize:" << windowSize
                 << std::endl << " - reps:" << reps;


    return stringStream.str();
}

std::vector<Runtime::Execution::RecordBuffer> MicroBenchmarkRun::createInputRecords(Runtime::BufferManagerPtr bufferManager) {
    return ASP::Util::createBuffersFromCSVFile(aggregation.inputFile, aggregation.inputSchema, bufferManager);
}

std::vector<Runtime::Execution::RecordBuffer> MicroBenchmarkRun::createAccuracyRecords(Runtime::BufferManagerPtr bufferManager) {
    return ASP::Util::createBuffersFromCSVFile(aggregation.accuracyFile, aggregation.outputSchema, bufferManager);
}

double MicroBenchmarkRun::compareAccuracy(std::vector<Runtime::Execution::RecordBuffer>& allAccuracyRecords,
                                          std::vector<Runtime::Execution::RecordBuffer>& allApproximateRecords) {

    double sumError = 0.0;
    auto numTuples = 0UL;
    for (auto& accBuf : allAccuracyRecords) {
        auto dynamicAccBuf = ASP::Util::createDynamicTupleBuffer((Runtime::TupleBuffer&)accBuf.getReference(),
                                                                 aggregation.outputSchema);
        for (auto& approxBuf : allApproximateRecords) {
            auto dynamicApproxBuf = ASP::Util::createDynamicTupleBuffer((Runtime::TupleBuffer&)approxBuf.getReference(),
                                                                        aggregation.outputSchema);
            NES_ASSERT(dynamicAccBuf.getNumberOfTuples() == dynamicApproxBuf.getNumberOfTuples(),
                       "Approximate Buffer and Accuracy Buffer must have the same number of tuples");

            for (auto i = 0UL; i < dynamicApproxBuf.getNumberOfTuples(); ++i) {
                auto accId = dynamicAccBuf[i]["id"];
                auto approxId = dynamicApproxBuf[i]["id"];
                if (accId == approxId) {
                    auto exactAgg = dynamicAccBuf[i][aggregation.fieldNameApproximate];
                    auto approxAgg = dynamicApproxBuf[i][aggregation.fieldNameApproximate];
                    sumError += std::abs(exactAgg.read<double>() - approxAgg.read<double>());
                    ++numTuples;
                }
            }
        }
    }

    return sumError / numTuples;
}
} // namespace NES::ASP::Benchmarking