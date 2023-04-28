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

#include <Experimental/Benchmarking/MicroBenchmarkASPUtil.hpp>
#include <Experimental/Benchmarking/MicroBenchmarkRun.hpp>
#include <Experimental/Synopses/AbstractSynopsis.hpp>
#include <Experimental/Operators/SynopsesOperator.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Pipelines/CompilationPipelineProvider.hpp>
#include <Execution/Pipelines/PhysicalOperatorPipeline.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Timer.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Util/yaml/Yaml.hpp>

namespace NES::ASP::Benchmarking {
void MicroBenchmarkRun::run() {

    Timer timer("benchmarkLoop");
    for (auto rep = 0UL; rep < reps; ++rep) {
        // Creating bufferManager for this run
        auto bufferManager = std::make_shared<Runtime::BufferManager>(bufferSize, numberOfBuffers);

        NES_INFO2("Creating input buffer...");
        auto allRecordBuffers = this->createInputRecords(bufferManager);

        NES_INFO2("Creating accuracy buffer...");
        auto allAccuracyBuffers = this->createAccuracyRecords(allRecordBuffers, bufferManager);

        // Create the synopsis, so we can call getApproximate after all buffers have been executed
        NES_INFO2("Creating the synopsis...");
        auto synopsis = AbstractSynopsis::create(*synopsesArguments, aggregation);
        synopsis->setBufferManager(bufferManager);

        // Create and compile the pipeline and create a workerContext
        NES_INFO2("Create and compile the pipeline...");
        auto [pipeline, pipelineContext] = createExecutablePipeline(synopsis);
        auto workerContext = std::make_shared<Runtime::WorkerContext>(0, bufferManager, 100);

        auto provider = Runtime::Execution::ExecutablePipelineProviderRegistry::getPlugin("PipelineCompiler").get();
        auto executablePipeline = provider->create(pipeline, Nautilus::CompilationOptions());

        // Creating strings here for the snapshots
        const std::string setupPipelineSnapshotName = "setupPipeline_loopIteration_" + std::to_string(rep + 1);
        const std::string executePipelineSnapshotName = "executePipeline_loopIteration_" + std::to_string(rep + 1);
        const std::string getApproximateSnapshotName = "getApproximate_loopIteration_" + std::to_string(rep + 1);

        timer.start();
        executablePipeline->setup(*pipelineContext);
        timer.snapshot(setupPipelineSnapshotName);

        for (auto& buffer : allRecordBuffers) {
            executablePipeline->execute(buffer, *pipelineContext, *workerContext);
        }

        timer.snapshot(executePipelineSnapshotName);
        auto allApproximateBuffers = synopsis->getApproximate(bufferManager);

        timer.snapshot(getApproximateSnapshotName);
        timer.pause();

        executablePipeline->stop(*pipelineContext);
        auto duration = timer.getRuntimeFromSnapshot(timer.createFullyQualifiedSnapShotName(executePipelineSnapshotName)) +
            timer.getRuntimeFromSnapshot(timer.createFullyQualifiedSnapShotName(getApproximateSnapshotName));

        // Checking for the accuracy and calculating the throughput of this loop
        auto accuracy = this->compareAccuracy(allAccuracyBuffers, allApproximateBuffers);

        auto throughputInTuples = windowSize / ((double)duration / NANO_TO_SECONDS_MULTIPLIER);
        NES_DEBUG2("accuracy: {} throughputInTuples: {}", accuracy, throughputInTuples);
        microBenchmarkResult.emplace_back(throughputInTuples, accuracy);
    }

    NES_INFO2("Loop timers: {}", printString(timer));
}

std::vector<MicroBenchmarkRun> MicroBenchmarkRun::parseMicroBenchmarksFromYamlFile(const std::string& yamlConfigFile,
                                                                                   const std::filesystem::path& absoluteDataPath) {
    std::vector<MicroBenchmarkRun> retVector;

    Yaml::Node configFile;
    Yaml::Parse(configFile, yamlConfigFile.c_str());
    if (configFile.IsNone()) {
        NES_THROW_RUNTIME_ERROR("Could not parse " << yamlConfigFile << "!");
    }

    // Parsing all required members from the yaml file
    auto parsedReps = ASP::Util::parseReps(configFile["reps"]);
    auto parsedSynopsisArguments = ASP::Util::parseSynopsisConfigurations(configFile["synopsis"]);
    auto parsedAggregations = ASP::Util::parseAggregations(configFile["aggregation"], absoluteDataPath);
    auto parsedWindowSizes = ASP::Util::parseWindowSizes(configFile["windowSize"]);
    auto parsedBufferSizes = ASP::Util::parseBufferSizes(configFile["bufferSize"]);
    auto parsedNumberOfBuffers = ASP::Util::parseNumberOfBuffers(configFile["numberOfBuffers"]);

    for (const auto& synopsisArgument : parsedSynopsisArguments) {
        for (const auto& [aggregation, inputFile] : parsedAggregations) {
            for (const auto& windowSize : parsedWindowSizes) {
                for (const auto& bufferSize : parsedBufferSizes) {
                    for (const auto& numberOfBuffers : parsedNumberOfBuffers) {
                        retVector.push_back(MicroBenchmarkRun(synopsisArgument, aggregation, bufferSize, numberOfBuffers,
                                                             windowSize, inputFile, parsedReps));
                    }
                }
            }
        }
    }

    return retVector;
}

MicroBenchmarkRun::MicroBenchmarkRun(Parsing::SynopsisConfigurationPtr synopsesArguments,
                                     const Parsing::SynopsisAggregationConfig& aggregation,
                                     const uint32_t bufferSize,
                                     const uint32_t numberOfBuffers,
                                     const size_t windowSize,
                                     const std::string& inputFile,
                                     const size_t reps)
    : synopsesArguments(std::move(synopsesArguments)), aggregation(aggregation), bufferSize(bufferSize),
      numberOfBuffers(numberOfBuffers), windowSize(windowSize), inputFile(inputFile), reps(reps){}

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
    inputFile = other.inputFile;
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
    inputFile = other.inputFile;
    reps = other.reps;
    microBenchmarkResult = std::vector(other.microBenchmarkResult);
}

const std::string MicroBenchmarkRun::getHeaderAsCsv() const {
    std::stringstream stringStream;
    stringStream << synopsesArguments->getHeaderAsCsv()
                 << "," << aggregation.getHeaderAsCsv()
                 << ",bufferSize"
                 << ",numberOfBuffers"
                 << ",windowSize"
                 << ",inputFile"
                 << ",reps";
    if (!microBenchmarkResult.empty()) {
        stringStream << "," << microBenchmarkResult[0].getHeaderAsCsv();
    }


    return stringStream.str();
}

const std::string MicroBenchmarkRun::getRowsAsCsv() const {
    std::stringstream stringStream;

    for (auto& benchmarkResult : microBenchmarkResult) {
        stringStream << synopsesArguments->getValuesAsCsv()
                     << "," << aggregation.getValuesAsCsv()
                     << "," << bufferSize
                     << "," << numberOfBuffers
                     << "," << windowSize
                     << "," << inputFile
                     << "," << reps
                     << "," << benchmarkResult.getRowAsCsv()
                     << std::endl;
    }
    return stringStream.str();
}

const std::string MicroBenchmarkRun::toString() const {
    std::stringstream stringStream;
    stringStream << std::endl << " - synopsis arguments: " << synopsesArguments->toString()
                 << std::endl << " - aggregation: " << aggregation.toString()
                 << std::endl << " - bufferSize : " << bufferSize
                 << std::endl << " - numberOfBuffers: " << numberOfBuffers
                 << std::endl << " - windowSize: " << windowSize
                 << std::endl << " - inputFile: " << inputFile
                 << std::endl << " - reps: " << reps;


    return stringStream.str();
}

std::vector<Runtime::TupleBuffer> MicroBenchmarkRun::createInputRecords(Runtime::BufferManagerPtr bufferManager) {
    return NES::Util::createBuffersFromCSVFile(inputFile, aggregation.inputSchema, bufferManager,
                                               aggregation.timeStampFieldName, windowSize);
}

std::vector<Runtime::TupleBuffer> MicroBenchmarkRun::createAccuracyRecords(std::vector<Runtime::TupleBuffer>& inputBuffers,
                                                                           Runtime::BufferManagerPtr bufferManager) {
    auto aggregationFunction = aggregation.createAggregationFunction();
    auto aggregationValue = aggregation.createAggregationValue();
    auto aggregationValueMemRef = Nautilus::MemRef((int8_t*)aggregationValue.get());

    auto memoryProvider = Runtime::Execution::MemoryProvider::MemoryProvider::createMemoryProvider(bufferManager->getBufferSize(), aggregation.inputSchema);


    // TODO for now we can ignore windows
    aggregationFunction->reset(aggregationValueMemRef);
    for (auto& buffer : inputBuffers) {
        NES_INFO2("buffer: {}", NES::Util::printTupleBufferAsCSV(buffer, aggregation.inputSchema));
        auto numberOfRecords = buffer.getNumberOfTuples();
        auto bufferAddress = Nautilus::Value<Nautilus::MemRef>((int8_t*) buffer.getBuffer());
        for (Nautilus::Value<Nautilus::UInt64> i = (uint64_t) 0; i < numberOfRecords; i = i + (uint64_t) 1) {
            auto record = memoryProvider->read({}, bufferAddress, i);
            auto value = record.read(aggregation.fieldNameAggregation);
            aggregationFunction->lift(aggregationValueMemRef, value);
        }
    }

    // Writes the output into a Nautilus::Record
    Nautilus::Record record;
    auto exactValue = aggregationFunction->lower(aggregationValueMemRef);
    record.write(aggregation.fieldNameApproximate, exactValue);

    // Create an output buffer and write the approximation into it
    auto outputMemoryProvider = Runtime::Execution::MemoryProvider::MemoryProvider::createMemoryProvider(bufferManager->getBufferSize(), aggregation.outputSchema);
    auto outputBuffer = bufferManager->getBufferBlocking();
    auto outputRecordBuffer = Nautilus::Value<Nautilus::MemRef>((int8_t*) outputBuffer.getBuffer());
    Nautilus::Value<Nautilus::UInt64> recordIndex(0UL);
    outputMemoryProvider->write(recordIndex, outputRecordBuffer, record);
    outputBuffer.setNumberOfTuples(1);

    return {outputBuffer};
}

double MicroBenchmarkRun::compareAccuracy(std::vector<Runtime::TupleBuffer>& allAccuracyRecords,
                                          std::vector<Runtime::TupleBuffer>& allApproximateBuffers) {

    double sumError = 0.0;
    auto numTuples = 0UL;

    for (auto& accBuf : allAccuracyRecords) {
        auto dynamicAccBuf = Runtime::MemoryLayouts::DynamicTupleBuffer::createDynamicTupleBuffer(accBuf, aggregation.outputSchema);
        NES_DEBUG2("dynamicAccBuf: {}", NES::Util::printTupleBufferAsCSV(dynamicAccBuf.getBuffer(), aggregation.outputSchema));
        for (auto& approxBuf : allApproximateBuffers) {
            auto dynamicApproxBuf = Runtime::MemoryLayouts::DynamicTupleBuffer::createDynamicTupleBuffer(approxBuf, aggregation.outputSchema);
            NES_DEBUG2("dynamicApproxBuf: {}", NES::Util::printTupleBufferAsCSV(dynamicApproxBuf.getBuffer(), aggregation.outputSchema));
            NES_ASSERT(dynamicAccBuf.getNumberOfTuples() == dynamicApproxBuf.getNumberOfTuples(),
                       "Approximate Buffer and Accuracy Buffer must have the same number of tuples");

            for (auto i = 0UL; i < dynamicApproxBuf.getNumberOfTuples(); ++i) {
                auto exactAgg = dynamicAccBuf[i][aggregation.fieldNameApproximate];
                auto approxAgg = dynamicApproxBuf[i][aggregation.fieldNameApproximate];

                sumError += Util::calculateRelativeError(approxAgg, exactAgg);
                ++numTuples;
            }
        }
    }

    return 1.0 - (sumError / numTuples);
}

std::pair<std::shared_ptr<Runtime::Execution::PhysicalOperatorPipeline>, std::shared_ptr<MockedPipelineExecutionContext>>
MicroBenchmarkRun::createExecutablePipeline(AbstractSynopsesPtr synopsis) {
    using namespace Runtime::Execution;

    // Scan Operator
    auto scanMemoryProvider = Runtime::Execution::MemoryProvider::MemoryProvider::createMemoryProvider(bufferSize, aggregation.inputSchema);
    auto scan = std::make_shared<Operators::Scan>(std::move(scanMemoryProvider));

    // Synopses Operator
    auto synopsesOperator = std::make_shared<Operators::SynopsesOperator>(synopsis);
    auto pipelineExecutionContext = std::make_shared<MockedPipelineExecutionContext>();

    // Build Pipeline
    auto pipeline = std::make_shared<PhysicalOperatorPipeline>();
    scan->setChild(synopsesOperator);
    pipeline->setRootOperator(scan);

    return std::make_pair(pipeline, pipelineExecutionContext);
}

} // namespace NES::ASP::Benchmarking