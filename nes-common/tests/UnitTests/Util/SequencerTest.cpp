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

#include <Sequencing/Sequencer.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <optional>
#include <random>
#include <ranges>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Sequencing/SequenceData.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>

using namespace ::testing;
namespace NES
{

class SequencerTest : public ::testing::Test
{
protected:
    void SetUp() override { }

    static void SetUpTestSuite()
    {
        NES::Logger::setupLogging("SequencerTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup SequencerTest test class.");
    }

    Sequencer<int> sequencer;
};

TEST_F(SequencerTest, InOrderSingleSequence)
{
    /// Test processing data in the correct order within a single sequence
    const SequenceData data1 = {INITIAL<SequenceNumber>, INITIAL<ChunkNumber>, false};
    const SequenceData data2 = {INITIAL<SequenceNumber>, ChunkNumber(ChunkNumber::INITIAL + 1), false};
    const SequenceData data3 = {INITIAL<SequenceNumber>, ChunkNumber(ChunkNumber::INITIAL + 2), true};

    auto result1 = sequencer.isNext(data1, 100);
    ASSERT_TRUE(result1.has_value());
    EXPECT_EQ(*result1, 100);

    result1 = sequencer.advanceAndGetNext(data1);
    EXPECT_FALSE(result1.has_value());

    auto result2 = sequencer.isNext(data2, 200);
    ASSERT_TRUE(result2.has_value());
    EXPECT_EQ(*result2, 200);

    result2 = sequencer.advanceAndGetNext(data2);
    EXPECT_FALSE(result2.has_value());

    auto result3 = sequencer.isNext(data3, 300);
    ASSERT_TRUE(result3.has_value());
    EXPECT_EQ(*result3, 300);

    result3 = sequencer.advanceAndGetNext(data3);
    EXPECT_FALSE(result3.has_value());
}

TEST_F(SequencerTest, MultipleSequences)
{
    /// Test processing multiple sequences
    const SequenceData seq1data1 = {INITIAL<SequenceNumber>, INITIAL<ChunkNumber>, true}; /// First sequence with single chunk
    const SequenceData seq2data1
        = {SequenceNumber(SequenceNumber::INITIAL + 1), INITIAL<ChunkNumber>, false}; /// Second sequence, first chunk
    const SequenceData seq2data2 = {
        SequenceNumber(SequenceNumber::INITIAL + 1), ChunkNumber(ChunkNumber::INITIAL + 1), true}; /// Second sequence, second chunk (last)

    auto result1 = sequencer.isNext(seq1data1, 100);
    ASSERT_TRUE(result1.has_value());
    EXPECT_EQ(*result1, 100);

    result1 = sequencer.advanceAndGetNext(seq1data1);
    EXPECT_FALSE(result1.has_value());

    auto result2 = sequencer.isNext(seq2data1, 200);
    ASSERT_TRUE(result2.has_value());
    EXPECT_EQ(*result2, 200);

    result2 = sequencer.advanceAndGetNext(seq2data1);
    EXPECT_FALSE(result2.has_value());

    auto result3 = sequencer.isNext(seq2data2, 300);
    ASSERT_TRUE(result3.has_value());
    EXPECT_EQ(*result3, 300);

    result3 = sequencer.advanceAndGetNext(seq2data2);
    EXPECT_FALSE(result3.has_value());
}

TEST_F(SequencerTest, OutOfOrderProcessing)
{
    /// Test that out-of-order data is properly stored and retrieved later
    const SequenceData data1 = {INITIAL<SequenceNumber>, INITIAL<ChunkNumber>, false};
    const SequenceData data2 = {INITIAL<SequenceNumber>, ChunkNumber(ChunkNumber::INITIAL + 1), false};
    const SequenceData data3 = {INITIAL<SequenceNumber>, ChunkNumber(ChunkNumber::INITIAL + 2), true};

    /// Submit data out of order: 2, 3, 1
    auto result2 = sequencer.isNext(data2, 200);
    EXPECT_FALSE(result2.has_value()); /// Not the next expected, should return empty

    auto result3 = sequencer.isNext(data3, 300);
    EXPECT_FALSE(result3.has_value()); /// Not the next expected, should return empty

    auto result1 = sequencer.isNext(data1, 100);
    ASSERT_TRUE(result1.has_value());
    EXPECT_EQ(*result1, 100);

    /// After processing data1, data2 should be automatically returned
    result1 = sequencer.advanceAndGetNext(data1);
    ASSERT_TRUE(result1.has_value());
    EXPECT_EQ(*result1, 200);

    /// After processing data2, data3 should be automatically returned
    result2 = sequencer.advanceAndGetNext(data2);
    ASSERT_TRUE(result2.has_value());
    EXPECT_EQ(*result2, 300);

    /// Finally process data3
    result3 = sequencer.advanceAndGetNext(data3);
    EXPECT_FALSE(result3.has_value());
}


class ConcurrentSequencerTest : public ::testing::TestWithParam<std::tuple<size_t, int, size_t>>
{
    void SetUp() override
    {
        std::tie(numSequences, maxNumberOfChunks, numThreads) = GetParam();
        sequencer = {};
    }

    void TearDown() override
    {
        const int pre = result[0];
        for (const int result1 : result | std::views::drop(1))
        {
            EXPECT_LT(pre, result1);
        }
    }

public:
    /// Process data according to the canonical usage pattern
    void processData(const std::vector<std::pair<SequenceData, std::pair<int, SequenceData>>>& dataChunks)
    {
        std::mt19937 gen{rd()};
        std::uniform_int_distribution<> dist{1, 10};

        for (const auto& [seq, value] : dataChunks)
        {
            /// Random delay to simulate real-world conditions
            std::this_thread::sleep_for(std::chrono::milliseconds(dist(gen)));

            /// Follow canonical usage pattern
            auto dataToProcess = sequencer.isNext(seq, value);
            while (dataToProcess)
            {
                processDataItem(dataToProcess->first);
                dataToProcess = sequencer.advanceAndGetNext(dataToProcess->second);
            }
        }
    }

    void processDataItem(int value)
    {
        /// Store the value in the result vector at the index equal to the value
        result[value] = value;
    }
    Sequencer<std::pair<int, SequenceData>> sequencer;
    int maxNumberOfChunks = 1000;
    size_t numSequences = 1000;
    int numThreads = 10;
    std::vector<int> result;

    /// Random generator for delays
    std::random_device rd;
};


TEST_P(ConcurrentSequencerTest, MultiThreaded)
{
    std::vector<std::tuple<SequenceData, std::pair<int, SequenceData>>> values;
    int counter = 0;
    for (auto i = SequenceNumber::INITIAL; i <= numSequences; i++)
    {
        auto noChunks = 1 + (rand() % maxNumberOfChunks);
        for (auto chunk = ChunkNumber::INITIAL; chunk < ChunkNumber::INITIAL + noChunks; ++chunk)
        {
            values.emplace_back(std::tuple<SequenceData, std::pair<int, SequenceData>>(
                /*sequence data*/ {SequenceNumber(i), ChunkNumber(chunk), false},
                {counter++, {SequenceNumber(i), ChunkNumber(chunk), false}}));
        }
        values.emplace_back(std::tuple<SequenceData, std::pair<int, SequenceData>>(
            /*sequence data*/ {SequenceNumber(i), ChunkNumber(noChunks + ChunkNumber::INITIAL), true},
            {counter++, {SequenceNumber(i), ChunkNumber(noChunks + ChunkNumber::INITIAL), true}}));
    }

    std::mt19937 randomGenerator(42);
    std::shuffle(values.begin(), values.end(), randomGenerator);
    result.resize(values.size());

    /// Split data among threads
    std::vector<std::vector<std::pair<SequenceData, std::pair<int, SequenceData>>>> threadData(numThreads);
    for (size_t i = 0; i < values.size(); ++i)
    {
        threadData[i % numThreads].emplace_back(values[i]);
    }

    /// Launch threads
    std::vector<std::thread> threads;
    threads.reserve(numThreads);
    for (int i = 0; i < numThreads; ++i)
    {
        threads.emplace_back(&ConcurrentSequencerTest::processData, this, threadData[i]);
    }

    /// Join all threads
    for (auto& thread : threads)
    {
        thread.join();
    }
}

INSTANTIATE_TEST_CASE_P(
    SequencerTest,
    ConcurrentSequencerTest,
    ::testing::Combine(::testing::Values(10, 100), ::testing::Values(1, 5), ::testing::Values(1, 4, 16)));

}
