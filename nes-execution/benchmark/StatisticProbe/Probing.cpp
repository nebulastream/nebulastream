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

#include <API/AttributeField.hpp>
#include <API/Expressions/Expressions.hpp>
#include <API/Expressions/LogicalExpressions.hpp>
#include <API/TestSchemas.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Formats/StatisticCollection/StatisticFormatFactory.hpp>
#include <StatisticCollection/StatisticStorage/AbstractStatisticStore.hpp>
#include <Statistics/Statistic.hpp>
#include <Util/Core.hpp>
#include <Util/StatisticProbeUtil.hpp>
#include <Util/StdInt.hpp>
#include <benchmark/benchmark.h>
#include <CreateProbePipelines.hpp>
#include <Execution/Pipelines/NautilusExecutablePipelineStage.hpp>
#include <Execution/Operators/Streaming/StatisticCollection/CountMin/CountMinProbe.hpp>
#include <Runtime/WorkerContext.hpp>
#include <future>
#include <csignal>
#include <gtest/gtest.h>
#include <Util/Timer.hpp>
#include <fstream>


namespace NES {

static constexpr auto SEED_RESERVOIR_SAMPLE = 42;
static constexpr auto SEED_COUNT_MIN = 42;
static auto inputSchema = TestSchemas::getSchemaTemplate("id_2val_time_u64")
                              ->addField("value3", BasicType::UINT64)
                              ->addField("value4", BasicType::UINT64)
                              ->addField("value5", BasicType::UINT64)
                              ->addField("value6", BasicType::UINT64)
                              ->updateSourceName("input");
static auto inputProbeSchema = TestSchemas::getSchemaTemplate("id_u64")->updateSourceName("inputProbe");


std::vector<Statistic::StatisticPtr>
createRandomCountMinSketches(uint64_t numberOfSketches, uint64_t width, uint64_t depth) {
    std::vector<Statistic::StatisticPtr> expectedStatistics;
    constexpr auto windowSize = 10;
    srand(SEED_COUNT_MIN);

    for (auto curCnt = 0_u64; curCnt < numberOfSketches; ++curCnt) {
        // Creating "random" values that represent of a CountMinStatistic for a tumbling window with a size of 10
        const Windowing::TimeMeasure startTs(windowSize * curCnt);
        const Windowing::TimeMeasure endTs(windowSize * (curCnt + 1));
        const uint64_t numberOfBitsInKey = 64;

        // We simulate a filling of count min by randomly updating the sketch
        const auto numberOfTuplesToInsert = rand() % 100 + 100;
        auto countMin = Statistic::CountMinStatistic::createInit(startTs, endTs, width, depth, numberOfBitsInKey)
                            ->as<Statistic::CountMinStatistic>();
        for (auto i = 0; i < numberOfTuplesToInsert; ++i) {
            for (auto row = 0_u64; row < depth; ++row) {
                auto rndCol = rand() % width;
                countMin->update(row, rndCol);
            }
        }

        // Creating now the expected CountMinStatistic
        expectedStatistics.emplace_back(countMin);
    }

    std::vector<Statistic::HashStatisticPair> statisticsWithHashes;
    std::transform(expectedStatistics.begin(), expectedStatistics.end(),
                   std::back_inserter(statisticsWithHashes),
                   [](const auto& statistic) {
                       static auto hash = 0;
                       return std::make_pair(++hash, statistic);
                   });
    return {expectedStatistics};
}

std::vector<Statistic::StatisticPtr>
createRandomReservoirSampleStatistics(uint64_t numberOfSamples,
                                      const SchemaPtr sampleSchema,
                                      uint64_t sampleSize) {
    std::vector<Statistic::StatisticPtr> expectedStatistics;
    constexpr auto windowSize = 10;
    srand(SEED_RESERVOIR_SAMPLE);

    for (auto curCnt = 0_u64; curCnt < numberOfSamples; ++curCnt) {
        // Creating "random" values that represent of a Reservoir Samples for a tumbling window with a size of 10
        const Windowing::TimeMeasure startTs(windowSize * curCnt);
        const Windowing::TimeMeasure endTs(windowSize * (curCnt + 1));
        auto sampleMemoryLayout = Util::createMemoryLayout(sampleSchema, sampleSize * sampleSchema->getSchemaSizeInBytes());

        // We simulate a filling of a sample by writing random values into the reservoir.
        const auto sample = Statistic::ReservoirSampleStatistic::createInit(startTs, endTs, sampleSize, sampleMemoryLayout->getSchema())->as<Statistic::ReservoirSampleStatistic>();
        auto* reservoirSpace = sample->getReservoirSpace();
        // We assume that every field is of type uint64
        for (auto i = 0_u64; i < sampleSize; ++i) {
            for (auto field = 0_u64; field < sampleMemoryLayout->getFieldSizes().size(); ++field) {
                int64_t rndVal = rand() % 10;
                const auto fieldOffSet = sampleMemoryLayout->getFieldOffset(i, field);
                *reinterpret_cast<uint64_t*>(reservoirSpace + fieldOffSet) = rndVal;
            }
        }
        sample->incrementObservedTuples(rand());

        // Creating now the expected ReservoirSamples
        expectedStatistics.emplace_back(sample);
    }


    std::vector<Statistic::HashStatisticPair> statisticsWithHashes;
    std::transform(expectedStatistics.begin(), expectedStatistics.end(),
                   std::back_inserter(statisticsWithHashes),
                   [](const auto& statistic) {
                       static auto hash = 0;
                       return std::make_pair(++hash, statistic);
                   });

    return {expectedStatistics};
}



/**Below are the loops for measuring the probe***/

static double BM_ProbeCPP_CountMin(uint64_t reps, uint64_t numberOfStatistics, uint64_t width, uint64_t depth, uint64_t numberOfProbeRequests) {
    Timer<> timer("execTimer");
    for (auto rep = 0_u64; rep < reps; ++rep) {
        auto countMinSketches = createRandomCountMinSketches(numberOfStatistics, width, depth);
        timer.start();
        for (auto probeRequest = 0_u64; probeRequest < numberOfProbeRequests; ++probeRequest) {
            auto expressionNodePtr = (Attribute("id") == probeRequest);
            auto probeExpression = Statistic::ProbeExpression(expressionNodePtr);
            for (const auto& countMin : countMinSketches) {
                auto statisticValue = Statistic::StatisticProbeUtil::probeCountMin(*countMin->as<Statistic::CountMinStatistic>(), probeExpression);
                ((void) statisticValue);
            }
        }
        timer.pause();
//        NES_WARNING("Done with rep: {}/{}", (rep+1), reps);
    }
    double timeInMs = timer.getPrintTime() / (double) reps;
    std::stringstream timeInMsStr;
    timeInMsStr << std::fixed << std::setprecision(3) << timeInMs << "ms";
    NES_WARNING("{}/{}/{}/{} took on average {}", numberOfStatistics, width, depth, numberOfProbeRequests, timeInMsStr.str());
    return timeInMs;
}

static double BM_ProbeCPP_ReservoirSample(uint64_t reps, uint64_t numberOfStatistics, uint64_t sampleSize, uint64_t numberOfProbeRequests) {
    Timer<> timer("execTimer");
    for (auto rep = 0_u64; rep < reps; ++rep) {
        auto reservoirSampleStatistics = createRandomReservoirSampleStatistics(numberOfStatistics, inputSchema, sampleSize);
        timer.start();
        for (auto probeRequest = 0_u64; probeRequest < numberOfProbeRequests; ++probeRequest) {
            auto expressionNodePtr = (Attribute("id") == probeRequest);
            auto probeExpression = Statistic::ProbeExpression(expressionNodePtr);
            for (const auto& reservoirSample : reservoirSampleStatistics) {
                auto memoryLayout =
                    Util::createMemoryLayout(inputSchema,
                                             reservoirSample->as<Statistic::ReservoirSampleStatistic>()->getSampleSize()
                                                 * inputSchema->getSchemaSizeInBytes());
                auto statisticValue = Statistic::StatisticProbeUtil::probeReservoirSample(
                    *reservoirSample->as<Statistic::ReservoirSampleStatistic>(),
                    probeExpression,
                    memoryLayout);
                ((void) statisticValue);
            }
        }
        timer.pause();
//        NES_WARNING("Done with rep: {}/{}", (rep+1), reps);
    }
    double timeInMs = timer.getPrintTime() / (double) reps;
    std::stringstream timeInMsStr;
    timeInMsStr << std::fixed << std::setprecision(3) << timeInMs << "ms";
    NES_WARNING("{}/{}/{} took on average {}", numberOfStatistics, sampleSize, numberOfProbeRequests, timeInMsStr.str());
    return timeInMs;
}


static double BM_ProbeQueryCompiler_CountMin(uint64_t reps, uint64_t numberOfStatistics, uint64_t width, uint64_t depth, uint64_t numberOfProbeRequests, std::string providerString) {
    using namespace NES::Runtime::Execution;
    Timer<> timer("execTimer");
    for (auto rep = 0_u64; rep < reps; ++rep) {
        // Create random CountMinSketches that we can probe
        auto countMinSketches = createRandomCountMinSketches(numberOfStatistics, width, depth);

        // Probe schema
        const auto keyFieldName = inputProbeSchema->getField("id")->getName();

        // Create the probe pipelines or the class for doing it
        CreateProbePipelines createProbePipelines;

        // Getting the pipeline compilation provider
        auto provider = ExecutablePipelineProviderRegistry::getPlugin(providerString).get();

        // Setting the compilation options
        Nautilus::CompilationOptions options;
        options.setDebug(false);
        options.setDumpToFile(false);
        options.setOptimize(true);

        // Resuming the time now
        timer.start();

        // Compiling the pipeline
        constexpr auto numberOfBitsInKey = sizeof(64) * 8;
        auto [pipelineBuild, probeOperatorHandler] = createProbePipelines.createCountMinProbePipeline(numberOfBitsInKey,
                                                                                                      inputProbeSchema,
                                                                                                      keyFieldName);
        auto compiledPipeline = provider->create(pipelineBuild, options);
        compiledPipeline->setup(*createProbePipelines.pipelineExecutionContext);
        timer.snapshot("compiling");


        // Probing
        for (auto probeRequest = 0_u64; probeRequest < numberOfProbeRequests; ++probeRequest) {
            auto buffer = createProbePipelines.bufferManager->getBufferBlocking();
            *reinterpret_cast<uint64_t*>(buffer.getBuffer()) = probeRequest;
            buffer.setNumberOfTuples(1);

            for (const auto& countMin : countMinSketches) {
                probeOperatorHandler->setNewStatistic(countMin->as<Statistic::CountMinStatistic>(), numberOfBitsInKey);
                // Probing with the tuple buffers
                compiledPipeline->execute(buffer,
                                          *createProbePipelines.pipelineExecutionContext,
                                          *createProbePipelines.workerContext);
                // Waiting for the result
                auto statisticValue = probeOperatorHandler->getStatisticValue();
                ((void) statisticValue);
                timer.snapshot("probing");
            }
        }
        timer.pause();
//        NES_WARNING("Done with rep: {}/{}", (rep+1), reps);
    }

    double timeInMs = timer.getPrintTime() / (double) reps;
    std::stringstream timeInMsStr;
    timeInMsStr << std::fixed << std::setprecision(3) << timeInMs << "ms";
    NES_WARNING("{}/{}/{}/{}/{} took on average {}", numberOfStatistics, width, depth, numberOfProbeRequests, providerString, timeInMsStr.str());
    return timeInMs;
}

static double BM_ProbeQueryCompiler_ReservoirSample(uint64_t reps, uint64_t numberOfStatistics, uint64_t sampleSize, uint64_t numberOfProbeRequests, std::string providerString) {
    using namespace NES::Runtime::Execution;
    Timer<> timer("execTimer");
    for (auto rep = 0_u64; rep < reps; ++rep) {
        // Create random ReservoirSample that we can probe
        auto reservoirSampleStatistics = createRandomReservoirSampleStatistics(numberOfStatistics, inputSchema, sampleSize);

        // Probe schema
        const auto fieldNameToCountSample = inputSchema->getField("id")->getName();
        const auto fieldNameToCountInput = inputProbeSchema->getField("id")->getName();

        // Create the probe pipelines or the class for doing it
        CreateProbePipelines createProbePipelines;

        // Getting the pipeline compilation provider
        auto provider = ExecutablePipelineProviderRegistry::getPlugin(providerString).get();

        // Setting the compilation options
        Nautilus::CompilationOptions options;
        options.setDebug(false);
        options.setDumpToFile(false);
        options.setOptimize(true);

        // Resuming the time now
        timer.start();

        // Compiling the pipeline
        auto [pipelineBuild, probeOperatorHandler] = createProbePipelines.createReservoirSamplePipeline(inputProbeSchema, inputSchema, fieldNameToCountInput, fieldNameToCountSample);
        auto compiledPipeline = provider->create(pipelineBuild, options);
        compiledPipeline->setup(*createProbePipelines.pipelineExecutionContext);
        timer.snapshot("compiling");

        // Probing
        for (auto probeRequest = 0_u64; probeRequest < numberOfProbeRequests; ++probeRequest) {
            auto buffer = createProbePipelines.bufferManager->getBufferBlocking();
            *reinterpret_cast<uint64_t*>(buffer.getBuffer()) = probeRequest;
            buffer.setNumberOfTuples(1);

            for (const auto& reservoirSample : reservoirSampleStatistics) {
                probeOperatorHandler->setNewStatistic(reservoirSample);

                // Probing with the tuple buffer
                    compiledPipeline->execute(buffer,
                                              *createProbePipelines.pipelineExecutionContext,
                                              *createProbePipelines.workerContext);
                // Waiting for the result
                auto statisticValue = probeOperatorHandler->getStatisticValue();
                ((void) statisticValue);
                timer.snapshot("probing");
            }
        }

        timer.pause();
//        NES_WARNING("Done with rep: {}/{}", (rep+1), reps);
    }

    double timeInMs = timer.getPrintTime() / (double) reps;
    std::stringstream timeInMsStr;
    timeInMsStr << std::fixed << std::setprecision(3) << timeInMs << "ms";
    NES_WARNING("{}/{}/{}/{} took on average {}", numberOfStatistics, sampleSize, numberOfProbeRequests, providerString, timeInMsStr.str());
    return timeInMs;
}

} // namespace NES

static constexpr auto NUM_REPS = 5;
static constexpr auto RESERVOIR_SAMPLE_CSV_FILE_NAME = "reservoir_sample.csv";
static constexpr auto COUNT_MIN_CSV_FILE_NAME = "count_min.csv";
int main(int, char**) {
    using namespace NES;

    // Calling NesLogger setup
    NES::Logger::setupLogging("blub.log", NES::LogLevel::LOG_WARNING);

    // General Parameters
    std::vector<uint64_t> allNumberOfStatistic =     {1, 10};                 // Number of statistics to be generated
    std::vector<uint64_t> allNumberOfProbeRequests = {1, 100, 1'000, 10'000}; // Number of probe request per sketch
    std::vector<std::string> allPipelineProviders =  {"PipelineCompiler", "CPPPipelineCompiler"}; // Pipeline provider


    // Count Min Parameters
    std::vector<uint64_t> allWidthsCM =              {10, 1024, 8196, 100 * 1024};        // Width of the CountMinSketch
    std::vector<uint64_t> allDepths =                {1, 5, 10, 100};                     // Depth of the CountMinSketch

    // Reservoir Sample Parameters
    std::vector<uint64_t> allSampleSizes =           {1, 100, 500, 1'000, 10'000, 100'000, 1'000'000};  // Sample Size


    // Truncating the csv files
    std::ofstream reservoirSampleFile(RESERVOIR_SAMPLE_CSV_FILE_NAME, std::ios::trunc);
    std::ofstream countMinFile(COUNT_MIN_CSV_FILE_NAME, std::ios::trunc);
    countMinFile << "numberOfStatistics,width,depth,numberOfProbeRequests,timeInMsPipelineCompiler,timeInMsCPPPipelineCompiler,timeInMsCPP" << std::endl;
    reservoirSampleFile << "numberOfStatistics,sampleSize,numberOfProbeRequests,timeInMsPipelineCompiler,timeInMsCPPPipelineCompiler,timeInMsCPP" << std::endl;


    for (const auto numberOfStatistics : allNumberOfStatistic) {
        for (const auto numberOfProbeRequests : allNumberOfProbeRequests) {
            for (const auto width : allWidthsCM) {
                for (const auto depth : allDepths) {
                    countMinFile << numberOfStatistics << "," << width << "," << depth << "," << numberOfProbeRequests << ",";

                    // Calling version with the QueryCompiler implementation of count min probe
                    for (const auto& provider : allPipelineProviders) {
//                        NES_WARNING("Starting with {}/{}/{}/{}/{}", numberOfStatistics, width, depth, numberOfProbeRequests, provider);
                        auto timeInMs = NES::BM_ProbeQueryCompiler_CountMin(NUM_REPS, numberOfStatistics, width, depth, numberOfProbeRequests, provider);
                        countMinFile << timeInMs << ",";
                    }

                    // Calling version with the CPP implementation of count min probe
//                    NES_WARNING("Starting with {}/{}/{}/{}", numberOfStatistics, width, depth, numberOfProbeRequests);
                    auto timeInMs = NES::BM_ProbeCPP_CountMin(NUM_REPS, numberOfStatistics, width, depth, numberOfProbeRequests);
                    countMinFile << timeInMs << std::endl;
                }
            }

            for (const auto sampleSize : allSampleSizes) {
                reservoirSampleFile << numberOfStatistics << "," << sampleSize << "," << numberOfProbeRequests << ",";

                // Calling version with the QueryCompiler implementation of reservoir sample probe
                for (const auto& provider : allPipelineProviders) {
//                    NES_WARNING("Starting with {}/{}/{}/{}", numberOfStatistics, sampleSize, numberOfProbeRequests, provider);
                    auto timeInMs = NES::BM_ProbeQueryCompiler_ReservoirSample(NUM_REPS, numberOfStatistics, sampleSize, numberOfProbeRequests, provider);
                    reservoirSampleFile << timeInMs << ",";
                }

                // Calling version with the CPP implementation of reservoir sample probe
//                NES_WARNING("Starting with {}/{}/{}", numberOfStatistics, sampleSize, numberOfProbeRequests);
                auto timeInMs = NES::BM_ProbeCPP_ReservoirSample(NUM_REPS, numberOfStatistics, sampleSize, numberOfProbeRequests);
                reservoirSampleFile << timeInMs << std::endl;
            }
        }
    }
}