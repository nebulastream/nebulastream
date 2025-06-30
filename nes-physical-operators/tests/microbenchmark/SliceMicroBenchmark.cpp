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

#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <random>
#include <ranges>
#include <Configurations/Worker/SliceCacheConfiguration.hpp>
#include <Nautilus/Interface/TimestampRef.hpp>
#include <Nautilus/Util.hpp>
#include <SliceCache/SliceCache.hpp>
#include <SliceCache/SliceCache2Q.hpp>
#include <SliceCache/SliceCacheFIFO.hpp>
#include <SliceCache/SliceCacheLFU.hpp>
#include <SliceCache/SliceCacheLRU.hpp>
#include <SliceCache/SliceCacheSecondChance.hpp>
#include <SliceCache/SliceCacheUtil.hpp>
#include <SliceStore/SliceAssigner.hpp>
#include <Time/Timestamp.hpp>
#include <fmt/ranges.h>
#include <magic_enum/magic_enum.hpp>

#include <Engine.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES
{

struct SliceCacheOptionsMicroBenchmark : public Configurations::SliceCacheOptions
{
    explicit SliceCacheOptionsMicroBenchmark(const Configurations::SliceCacheType sliceCacheType, uint64_t numberOfEntries)
        : Configurations::SliceCacheOptions({sliceCacheType, numberOfEntries})
    {
    }
    std::string getValuesAsCsv() const { return fmt::format("{},{}", magic_enum::enum_name(sliceCacheType), numberOfEntries); }
    std::string getOptimalValuesAsCsv() const { return fmt::format("Optimal,{}", numberOfEntries); }
    static std::string getCsvHeader() { return fmt::format("sliceCacheType,numberOfEntries"); }
};

size_t getSliceCacheEntrySize(const Configurations::SliceCacheOptions& sliceCacheOptions)
{
    switch (sliceCacheOptions.sliceCacheType)
    {
        case NES::Configurations::SliceCacheType::NONE:
            throw InvalidConfigParameter("SliceCacheType is None");
        case NES::Configurations::SliceCacheType::FIFO:
            return sizeof(SliceCacheEntryFIFO);
        case NES::Configurations::SliceCacheType::TWO_QUEUES:
            return sizeof(SliceCache2Q);
        case NES::Configurations::SliceCacheType::LRU:
            return sizeof(SliceCacheEntryLRU);
        case NES::Configurations::SliceCacheType::SECOND_CHANCE:
            return sizeof(SliceCacheEntrySecondChance);
    }
    std::unreachable();
}

/// Nautilus variant of @class SliceAssigner
class SliceAssignerRef
{
    nautilus::val<Timestamp::Underlying> windowSize;
    nautilus::val<Timestamp::Underlying> windowSlide;

public:
    explicit SliceAssignerRef(
        const nautilus::val<Timestamp::Underlying>& windowSize, const nautilus::val<Timestamp::Underlying>& windowSlide)
        : windowSize(windowSize), windowSlide(windowSlide)
    {
    }

    /**
     * @brief Calculates the start of a slice for a specific timestamp ts.
     * @param ts the timestamp for which we calculate the start of the particular slice.
     * @return Timestamp::Underlying slice start
     */
    [[nodiscard]] nautilus::val<Timestamp::Underlying> getSliceStartTs(const nautilus::val<Timestamp>& ts) const
    {
        const auto timestampRaw = ts.convertToValue();
        const auto prevSlideStart = timestampRaw - ((timestampRaw) % windowSlide);
        const auto prevWindowStart
            = timestampRaw < windowSize ? prevSlideStart : timestampRaw - ((timestampRaw - windowSize) % windowSlide);

        if (prevSlideStart < prevWindowStart)
        {
            return prevWindowStart;
        }
        return prevSlideStart;
    }

    /**
     * @brief Calculates the end of a slice for a specific timestamp ts.
     * @param ts the timestamp for which we calculate the end of the particular slice.
     * @return Timestamp::Underlying slice end
     */
    [[nodiscard]] nautilus::val<Timestamp::Underlying> getSliceEndTs(const nautilus::val<Timestamp>& ts) const
    {
        const auto timestampRaw = ts.convertToValue();
        const auto nextSlideEnd = timestampRaw + windowSlide - ((timestampRaw) % windowSlide);
        const auto nextWindowEnd
            = timestampRaw < windowSize ? windowSize : timestampRaw + windowSlide - ((timestampRaw - windowSize) % windowSlide);
        if (nextSlideEnd < nextWindowEnd)
        {
            return nextSlideEnd;
        }
        return nextWindowEnd;
    }
};

auto createSliceCacheFillFunction(
    const nautilus::engine::NautilusEngine& nautilusEngine, const Configurations::SliceCacheOptions& sliceCacheOptions)
{
    return nautilusEngine.registerFunction(std::function(
        [copyOfSliceCacheOptions = sliceCacheOptions](
            nautilus::val<Timestamp*> inputData,
            nautilus::val<uint64_t> sizeInputData,
            nautilus::val<int8_t*> startOfSliceEntries,
            nautilus::val<Timestamp::Underlying> windowSize,
            nautilus::val<Timestamp::Underlying> windowSlide)
        {
            using namespace nautilus;
            /// Creating a slice cache and filling all input timestamps
            /// We are not using the operator handler, therefore we set it to nullptr
            const nautilus::val<int8_t*> globalOperatorHandler = nullptr;
            const auto sliceCache = NES::Util::createSliceCache(copyOfSliceCacheOptions, globalOperatorHandler, startOfSliceEntries);
            for (val<uint64_t> i = 0; i < sizeInputData; ++i)
            {
                val<Timestamp> ts(Nautilus::Util::readValueFromMemRef<Timestamp::Underlying>(inputData + i));
                {
                    const SliceAssignerRef sliceAssigner(windowSize, windowSlide);
                    const auto sliceStart = sliceAssigner.getSliceStartTs(ts);
                    const auto sliceEnd = sliceAssigner.getSliceEndTs(ts);
                    // NES_INFO_EXEC("Trying to access slice " << sliceStart << "-" << sliceEnd << " for ts " << ts);
                }
                sliceCache->getDataStructureRef(
                    ts,
                    [&](const val<SliceCacheEntry*>& sliceCacheEntryToReplace, const nautilus::val<uint64_t>& replacementIndex)
                    {
                        /// To simulate adding a slice, we use a slice assigner to create slice start and end
                        const SliceAssignerRef sliceAssigner(windowSize, windowSlide);
                        const auto sliceStart = sliceAssigner.getSliceStartTs(ts);
                        const auto sliceEnd = sliceAssigner.getSliceEndTs(ts);


                        const auto sliceStartReplacement = sliceCache->getSliceStart(replacementIndex);
                        const auto sliceEndReplacement = sliceCache->getSliceEnd(replacementIndex);
                        // NES_INFO_EXEC(
                        //     "Adding " << sliceStart << "-" << sliceEnd << " for ts " << ts << "\t\t\tReplacing "
                        //               << sliceStartReplacement << "-" << sliceEndReplacement << " at replacementIndex "
                        //               << replacementIndex);


                        /// Writing the slice start and end. We assume first the slice start and then the slice end
                        const auto sliceStartMemory = Nautilus::Util::getMemberRef(sliceCacheEntryToReplace, &SliceCacheEntry::sliceStart);
                        const auto sliceEndMemory = Nautilus::Util::getMemberRef(sliceCacheEntryToReplace, &SliceCacheEntry::sliceEnd);
                        *static_cast<nautilus::val<uint64_t*>>(sliceStartMemory) = sliceStart;
                        *static_cast<nautilus::val<uint64_t*>>(sliceEndMemory) = sliceEnd;

                        /// Write any value to the datastructure
                        auto dataStructureMemory = Nautilus::Util::getMemberRef(sliceCacheEntryToReplace, &SliceCacheEntry::dataStructure);
                        *dataStructureMemory = nautilus::val<uint64_t>(0x12345678);

                        return sliceCacheEntryToReplace;
                    });
            }
        }));
}

/// This function creates timestamps following the "Poster: Generating Reproducible Out-of-Order Data Streams" by Grulich et al.
/// Philipp M. Grulich, Jonas Traub, Sebastian Breß, Asterios Katsifodimos, Volker Markl, and Tilmann Rabl. 2019.
/// Generating Reproducible Out-of-Order Data Streams. In Proceedings of the 13th ACM International Conference on Distributed and
/// Event-based Systems (DEBS '19). Association for Computing Machinery, New York, NY, USA, 256–257. https://doi.org/10.1145/3328905.3332511
std::vector<Timestamp> createTimestampsOutOfOrderDataStreams(
    const size_t numberOfTimestamps, const size_t minDelay, const size_t maxDelay, const double outOfOrderPercentage)
{
    /// Creating a vector containing strong monotonic timestamps of pairs (ingestion time, event time)
    std::vector<std::pair<Timestamp, Timestamp>> ingestionAndEventTime;
    ingestionAndEventTime.reserve(numberOfTimestamps);
    for (size_t i = 0; i < numberOfTimestamps; ++i)
    {
        ingestionAndEventTime.emplace_back(i, i);
    }

    /// We use a constant seed to ensure determinism
    constexpr auto seed = 42;
    std::mt19937 gen(seed);
    std::uniform_int_distribution<size_t> distribution(minDelay, maxDelay);
    std::uniform_real_distribution<> outOfOrder(0.0, 1.0);

    /// Generating out-of-order ingestion time
    uint64_t cnt = 0;
    for (size_t i = 0; i < numberOfTimestamps; ++i)
    {
        if (outOfOrder(gen) < outOfOrderPercentage)
        {
            const auto delay = distribution(gen);
            ingestionAndEventTime[i].first = ingestionAndEventTime[i].first + delay;
            ++cnt;
        }
    }

    /// Sorting according to the ingestion time to create the out-of-orderness
    std::ranges::sort(ingestionAndEventTime, [](const auto& t1, const auto& t2) { return t1.first < t2.first; });

    /// Create a new timestamp vector of only the event time
    std::vector<Timestamp> timestamps;
    timestamps.reserve(numberOfTimestamps);
    std::ranges::transform(ingestionAndEventTime, std::back_inserter(timestamps), [](const auto& ts) { return ts.second; });

    return timestamps;
}

std::vector<Timestamp>
createTimestamps(const size_t numberOfMeans, const size_t samplesPerMean, const double standardDeviation, const size_t meanStep)
{
    /// Creating a vector containing strong monotonic timestamps
    std::vector<Timestamp> timestamps;
    timestamps.reserve(numberOfMeans * samplesPerMean);

    /// We create #samplesPerMeans for #numberOfTimestamps around a normal_distribution with #stddev
    std::random_device rd;
    std::mt19937 gen(rd());

    for (size_t i = 0; i < numberOfMeans; ++i)
    {
        constexpr auto startMean = 1000;
        const double mean = startMean + i * meanStep;
        std::normal_distribution<> dist(mean, standardDeviation);
        for (size_t n = 0; n < samplesPerMean; ++n)
        {
            const auto sample = std::max(dist(gen), 0.0);
            timestamps.emplace_back(static_cast<size_t>(sample));
        }
    }

    return timestamps;
}

std::pair<uint64_t, uint64_t> calculateOptimalHistsMisses(
    const std::vector<Timestamp>& timestamps,
    const uint64_t maxCacheSize,
    const std::pair<Timestamp::Underlying, Timestamp::Underlying>& windowSizeSlide)
{
    std::vector<Timestamp> optimalCache;
    uint64_t cacheHits = 0;
    uint64_t cacheMisses = 0;
    for (size_t tsIndex = 0; tsIndex < timestamps.size(); ++tsIndex)
    {
        SliceAssigner sliceAssigner(windowSizeSlide.first, windowSizeSlide.second);
        const auto sliceStart = sliceAssigner.getSliceStartTs(timestamps[tsIndex]);
        if (optimalCache.empty() or std::ranges::find(optimalCache, sliceStart) == optimalCache.end())
        {
            /// Cache miss
            ++cacheMisses;
            if (optimalCache.size() == maxCacheSize)
            {
                /// Find the sliceStart in the cache that will be never accessed again or the furthest in the future
                std::optional<Timestamp> candidate;
                std::optional<std::ptrdiff_t> closestToEnd;
                for (const auto& sliceInCache : optimalCache)
                {
                    auto it = std::find_if(
                        timestamps.begin() + tsIndex,
                        timestamps.end(),
                        [sliceInCache, sliceAssigner](const Timestamp& ts)
                        {
                            const auto sliceStartOfTsAfterTsIndex = sliceAssigner.getSliceStartTs(ts);
                            return sliceInCache == sliceStartOfTsAfterTsIndex;
                        });
                    if (it == timestamps.end())
                    {
                        /// This slice will not be used again, remove it
                        candidate = sliceInCache;
                        break;
                    }
                    const auto distanceToEnd = std::distance(it, timestamps.end());
                    if (not closestToEnd or distanceToEnd < closestToEnd.value())
                    {
                        closestToEnd = distanceToEnd;
                        candidate = sliceInCache;
                    }
                }
                if (candidate.has_value())
                {
                    optimalCache.erase(std::ranges::find(optimalCache, candidate.value()));
                }
                else
                {
                    throw UnknownException("We should always erase an element!");
                }
            }
            optimalCache.emplace_back(sliceStart);
        }
        else
        {
            /// Cache hit
            ++cacheHits;
        }
    }

    return {cacheHits, cacheMisses};
}

struct BenchmarkParameters
{
    SliceCacheOptionsMicroBenchmark sliceCacheOptions;
    double outOfOrderPercentage;
    size_t numberOfTimestamps;
    size_t minDelay;
    size_t maxDelay;
    std::pair<Timestamp::Underlying, Timestamp::Underlying> windowSizeSlide;
    std::string getValuesAsCsv() const
    {
        return fmt::format(
            "{},{},{},{},{},{},{}",
            sliceCacheOptions.getValuesAsCsv(),
            outOfOrderPercentage,
            numberOfTimestamps,
            minDelay,
            maxDelay,
            windowSizeSlide.first,
            windowSizeSlide.second);
    }

    std::string getOptimalValuesAsCsv() const
    {
        return fmt::format(
            "{},{},{},{},{},{},{}",
            sliceCacheOptions.getOptimalValuesAsCsv(),
            outOfOrderPercentage,
            numberOfTimestamps,
            minDelay,
            maxDelay,
            windowSizeSlide.first,
            windowSizeSlide.second);
    }

    static std::string getCsvHeader()
    {
        return fmt::format(
            "{},outOfOrderPercentage,numberOfTimestamps,minDelay,maxDelay,windowSize,windowSlide",
            SliceCacheOptionsMicroBenchmark::getCsvHeader());
    }

    BenchmarkParameters(
        SliceCacheOptionsMicroBenchmark sliceCacheOptions,
        const double outOfOrderPercentage,
        const size_t numberOfTimestamps,
        const size_t minDelay,
        const size_t maxDelay,
        const std::pair<Timestamp::Underlying, Timestamp::Underlying>& windowSizeSlide)
        : sliceCacheOptions(std::move(sliceCacheOptions))
        , outOfOrderPercentage(outOfOrderPercentage)
        , numberOfTimestamps(numberOfTimestamps)
        , minDelay(minDelay)
        , maxDelay(maxDelay)
        , windowSizeSlide(windowSizeSlide)
    {
    }
};

struct BenchmarkRunMeasurements
{
    std::chrono::microseconds executionTime;
    uint64_t cacheHits;
    uint64_t cacheMisses;
    uint64_t optimalCacheHits;
    uint64_t optimalCacheMisses;

    std::string getValuesAsCsv() const
    {
        return fmt::format("{},{},{},{},{}", executionTime.count(), cacheHits, cacheMisses, optimalCacheHits, optimalCacheMisses);
    }
    std::string getOptimalValuesAsCsv() const
    {
        return fmt::format(
            "{},{},{},{},{}", executionTime.count(), optimalCacheHits, optimalCacheMisses, optimalCacheHits, optimalCacheMisses);
    }
    static std::string getCsvHeader() { return "executionTime,cacheHits,cacheMisses,optimalCacheHits,optimalCacheMisses"; }
};

std::vector<BenchmarkRunMeasurements> runBenchmark(const BenchmarkParameters& benchmarkParams, const int numReps)
{
    /// 1. Create a slice cache query compiled nautilus function that access the slice cache
    nautilus::engine::Options options;
    options.setOption("engine.Compilation", true);
    const nautilus::engine::NautilusEngine nautilusEngine(options);
    auto sliceCacheFunction = createSliceCacheFillFunction(nautilusEngine, benchmarkParams.sliceCacheOptions);

    std::vector<BenchmarkRunMeasurements> benchmarkRunMeasurements;
    for (auto rep = 0; rep < numReps; ++rep)
    {
        /// 2. Create timestamps / input data
        // auto timestamps = createTimestamps(
        // benchmarkParams.numberOfMeans, benchmarkParams.samplesPerMean, benchmarkParams.standardDeviation, benchmarkParams.meanStep);
        auto timestamps = createTimestampsOutOfOrderDataStreams(
            benchmarkParams.numberOfTimestamps, benchmarkParams.minDelay, benchmarkParams.maxDelay, benchmarkParams.outOfOrderPercentage);

        /// 3. Create space for slice cache
        const auto neededSize
            = benchmarkParams.sliceCacheOptions.numberOfEntries * getSliceCacheEntrySize(benchmarkParams.sliceCacheOptions)
            + sizeof(HitsAndMisses);
        std::vector<int8_t> sliceCacheMemory(neededSize);
        std::memset(sliceCacheMemory.data(), 0, neededSize);

        /// 4. Measure the execution time via chrono system clock for a particular slice cache
        const auto startTime = std::chrono::high_resolution_clock::now();
        sliceCacheFunction(
            timestamps.data(),
            timestamps.size(),
            sliceCacheMemory.data(),
            benchmarkParams.windowSizeSlide.first,
            benchmarkParams.windowSizeSlide.second);
        const auto duration = std::chrono::high_resolution_clock::now() - startTime;

        /// 5. Retrieve the cache hits and misses. We assume the hits and misses are stored in this order at the beginning of the sliceCacheMemory
        /// Thus, we simply need to read twice 64bits from the start of sliceCacheMemory
        const auto hits = *reinterpret_cast<uint64_t*>(sliceCacheMemory.data());
        const auto misses = *reinterpret_cast<uint64_t*>(sliceCacheMemory.data() + sizeof(hits));

        /// 6. Calculate the optimial cache hits and misses by using Belady's algorithm, also known as the OPT (Optimal Page Replacement) algorithm
        const auto& [optimalHits, optimalMisses]
            = calculateOptimalHistsMisses(timestamps, benchmarkParams.sliceCacheOptions.numberOfEntries, benchmarkParams.windowSizeSlide);

        /// 7. Returning the measurments
        benchmarkRunMeasurements.emplace_back(duration_cast<std::chrono::microseconds>(duration), hits, misses, optimalHits, optimalMisses);
    }

    return benchmarkRunMeasurements;
}

std::string createNewCsvFileLine(const BenchmarkParameters& parameters, const BenchmarkRunMeasurements& measurements)
{
    std::stringstream csvValues;
    /// We first write all values of parameters as csv, followed by the measurments
    csvValues << parameters.getValuesAsCsv();
    csvValues << ",";
    csvValues << measurements.getValuesAsCsv();

    /// Now we do the same for the optimal way
    csvValues << std::endl;
    csvValues << parameters.getOptimalValuesAsCsv();
    csvValues << ",";
    csvValues << measurements.getOptimalValuesAsCsv();
    return csvValues.str();
}

std::string createNewCsvHeaderLine()
{
    return BenchmarkParameters::getCsvHeader() + "," + BenchmarkRunMeasurements::getCsvHeader();
}

class ETACalculator
{
public:
    explicit ETACalculator(const int totalCalls) : totalCalls(totalCalls), callsCompleted(0)
    {
        startTime = std::chrono::system_clock::now();
    }

    void update()
    {
        const auto now = std::chrono::system_clock::now();
        const auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - startTime).count();
        const auto lastIterationElapsed = std::chrono::duration_cast<std::chrono::seconds>(now - lastIterationStart).count();

        callsCompleted++;
        lastIterationStart = now;

        if (elapsed > 0)
        {
            const double timePerCall = static_cast<double>(elapsed) / callsCompleted;
            const int remainingCalls = totalCalls - callsCompleted;
            const double etaSeconds = timePerCall * remainingCalls;

            /// Calculate ETA wall time
            const auto etaWallTime = std::chrono::system_clock::now() + std::chrono::seconds(static_cast<int>(etaSeconds));
            const std::time_t etaTimeT = std::chrono::system_clock::to_time_t(etaWallTime);

            /// Print progress, last iteration time, ETA in seconds, and ETA wall time
            std::cout << "Progress: " << callsCompleted << "/" << totalCalls << ". ";
            std::cout << "Last iteration took: " << lastIterationElapsed << " s. ";
            std::cout << "ETA: " << static_cast<int>(etaSeconds) << " seconds remaining. ";
            std::cout << "Wall time: " << std::put_time(std::localtime(&etaTimeT), "%Y-%m-%d %X") << std::endl;
        }
    }

private:
    int totalCalls;
    int callsCompleted;
    std::chrono::system_clock::time_point startTime;
    std::chrono::system_clock::time_point lastIterationStart;
};

}

int main()
{
    /// All benchmark parameters
    constexpr auto allSliceCacheTypes = magic_enum::enum_values<NES::Configurations::SliceCacheType>();
    const auto allSliceCacheSizes = {2, 5, 10, 20, 50, 100};
    const auto allNumberOfTimestamps = {100 * 1000};
    const std::vector<std::pair<size_t, size_t>> allMinMaxDelay = {{10, 1000}, {1000, 100 * 1000}, {100 * 1000, 10 * 1000 * 1000}};
    const auto allOutOfOrderPercentage = {0.1, 0.5, 0.9};
    const std::vector<std::pair<NES::Timestamp::Underlying, NES::Timestamp::Underlying>> allWindowSizeSlide
        = {{100, 100}, {1000, 1000}, {10 * 1000, 10 * 1000}};
    constexpr auto NUM_EXPERIMENT_RUNS = 3;
    std::filesystem::path csvFilePath("slice-cache-micro-benchmarks.csv");
    std::filesystem::remove(csvFilePath);
    std::ofstream csvFile(csvFilePath, std::ios::out | std::ios::app);


    NES::ETACalculator etaCalculator(
        allSliceCacheSizes.size() * allSliceCacheTypes.size() * allOutOfOrderPercentage.size() * allWindowSizeSlide.size()
        * allMinMaxDelay.size() * allNumberOfTimestamps.size());
    csvFile << NES::createNewCsvHeaderLine() << std::endl;
    std::cout << NES::createNewCsvHeaderLine() << std::endl;
    for (const auto& sliceCacheType : allSliceCacheTypes)
    {
        if (sliceCacheType == NES::Configurations::SliceCacheType::NONE)
        {
            /// We skip the slice cache type none
            continue;
        }
        NES::Logger::setupLogging(
            fmt::format("slice-cache-micro-benchmarks-{}.log", magic_enum::enum_name(sliceCacheType)), NES::LogLevel::LOG_TRACE, false);
        for (const uint64_t sliceCacheSize : allSliceCacheSizes)
        {
            for (const auto windowSizeSlide : allWindowSizeSlide)
            {
                for (const auto& [minDelay, maxDelay] : allMinMaxDelay)
                {
                    for (const auto& outOfOrderPercentage : allOutOfOrderPercentage)
                    {
                        for (const size_t numberOfTimestamps : allNumberOfTimestamps)
                        {
                            /// Running the benchmark
                            NES::BenchmarkParameters benchmarkParams{
                                NES::SliceCacheOptionsMicroBenchmark{sliceCacheType, sliceCacheSize},
                                outOfOrderPercentage,
                                numberOfTimestamps,
                                minDelay,
                                maxDelay,
                                windowSizeSlide};
                            const auto allMeasurements = NES::runBenchmark(benchmarkParams, NUM_EXPERIMENT_RUNS);
                            NES_INFO("Currently running: {}", benchmarkParams.getValuesAsCsv());

                            /// Create new csv file from benchmark params and measurements
                            for (const auto& measurement : allMeasurements)
                            {
                                csvFile << NES::createNewCsvFileLine(benchmarkParams, measurement) << std::endl;
                                std::cout << NES::createNewCsvFileLine(benchmarkParams, measurement) << std::endl;
                            }
                            csvFile.flush();
                            etaCalculator.update();
                        }
                    }
                }
            }
        }
    }
}
