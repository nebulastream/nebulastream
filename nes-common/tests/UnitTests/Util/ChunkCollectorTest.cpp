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
#include <Sequencing/ChunkCollector.hpp>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <iostream>
#include <limits>
#include <optional>
#include <random>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Time/Timestamp.hpp>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

using namespace ::testing;
namespace NES
{

class ChunkCollectorTest : public ::testing::Test
{
};

static auto SeqWithWatermark(SequenceNumber seq, Timestamp watermark)
{
    return Optional(Pair(seq, watermark));
}

TEST(ChunkCollectorTest, SingleInsert)
{
    ChunkCollector sequence;
    ASSERT_THAT(
        sequence.collect({INITIAL<SequenceNumber>, INITIAL<ChunkNumber>, true}, Timestamp(32)),
        SeqWithWatermark(INITIAL<SequenceNumber>, Timestamp(32)));
}

TEST(ChunkCollectorTest, MultipleChunks)
{
    ChunkCollector sequence;
    EXPECT_EQ(sequence.collect({INITIAL<SequenceNumber>, INITIAL<ChunkNumber>, false}, Timestamp(2)), std::nullopt);
    ASSERT_THAT(
        sequence.collect({INITIAL<SequenceNumber>, ChunkNumber(ChunkNumber::INITIAL + 1), true}, Timestamp(12)),
        SeqWithWatermark(INITIAL<SequenceNumber>, Timestamp(12)));
}

TEST(ChunkCollectorTest, MultipleChunksOutOfOrder)
{
    ChunkCollector sequence;
    EXPECT_EQ(sequence.collect({INITIAL<SequenceNumber>, ChunkNumber(ChunkNumber::INITIAL + 1), true}, Timestamp(42)), std::nullopt);
    ASSERT_THAT(
        sequence.collect({INITIAL<SequenceNumber>, INITIAL<ChunkNumber>, false}, Timestamp(2)),
        SeqWithWatermark(INITIAL<SequenceNumber>, Timestamp(42)));
}

TEST(CheckChunkCollector, MultipleTimesLastChunkSet)
{
    SKIP_IF_TSAN();

    ChunkCollector sequence;
    EXPECT_EQ(sequence.collect({INITIAL<SequenceNumber>, INITIAL<ChunkNumber>, false}, Timestamp(2)), std::nullopt);
    ASSERT_THAT(
        sequence.collect({INITIAL<SequenceNumber>, ChunkNumber(ChunkNumber::INITIAL + 1), true}, Timestamp(12)),
        SeqWithWatermark(INITIAL<SequenceNumber>, Timestamp(12)));
    EXPECT_DEATH_DEBUG(sequence.collect({INITIAL<SequenceNumber>, ChunkNumber(ChunkNumber::INITIAL + 2), true}, Timestamp(12)), "");
}

TEST(ChunkCollectorTest, DifferentSequenceNumbers)
{
    ChunkCollector sequence;
    EXPECT_EQ(sequence.collect({INITIAL<SequenceNumber>, ChunkNumber(ChunkNumber::INITIAL + 1), true}, Timestamp(32)), std::nullopt);
    EXPECT_EQ(sequence.collect({SequenceNumber(101), INITIAL<ChunkNumber>, false}, Timestamp(32)), std::nullopt);
    ASSERT_THAT(
        sequence.collect({INITIAL<SequenceNumber>, INITIAL<ChunkNumber>, false}, Timestamp(32)),
        SeqWithWatermark(INITIAL<SequenceNumber>, Timestamp(32)));
    ASSERT_THAT(
        sequence.collect({SequenceNumber(101), ChunkNumber(ChunkNumber::INITIAL + 1), true}, Timestamp(32)),
        SeqWithWatermark(SequenceNumber(101), Timestamp(32)));
}

class ConcurrentChunkCollectorTest
    : public ::testing::TestWithParam<std::tuple<SequenceNumber::Underlying, ChunkNumber::Underlying, size_t>>
{
};

TEST_P(ConcurrentChunkCollectorTest, RandomInserts)
{
    auto [maxSequenceNumber, maxChunkNumber, numberOfThreads] = GetParam();
    std::random_device rd;
    std::uniform_int_distribution<ChunkNumber::Underlying> chunkNumbers(ChunkNumber::INITIAL + 1, maxChunkNumber + ChunkNumber::INITIAL);
    std::uniform_int_distribution<Timestamp::Underlying> watermarks(1, 10000000);
    std::unordered_map<SequenceNumber::Underlying, Timestamp::Underlying> maxWaterMark;

    /// Prepare Inserts
    std::vector<std::pair<SequenceData, Timestamp::Underlying>> inserts;
    for (size_t i = SequenceNumber::INITIAL; i < maxSequenceNumber + SequenceNumber::INITIAL; ++i)
    {
        auto maxWatermarkForCurrentSequence = std::numeric_limits<Timestamp::Underlying>::min();
        auto chunks = chunkNumbers(rd);
        auto watermark = watermarks(rd);
        for (size_t j = ChunkNumber::INITIAL; j < chunks; ++j)
        {
            inserts.push_back({{SequenceNumber(i), ChunkNumber(j), false}, watermark});
            maxWatermarkForCurrentSequence = std::max(watermark, maxWatermarkForCurrentSequence);
        }
        maxWaterMark[i] = maxWatermarkForCurrentSequence;
        inserts.back().first.lastChunk = true;
    }

    /// Add inserts so they can be evenly divided on all threads
    auto moreInserts = numberOfThreads - (inserts.size() % numberOfThreads);
    auto maxWatermarkForLastSequence = std::numeric_limits<Timestamp::Underlying>::min();
    for (size_t i = ChunkNumber::INITIAL; i < moreInserts + ChunkNumber::INITIAL; ++i)
    {
        auto watermark = watermarks(rd);
        inserts.push_back({{SequenceNumber(maxSequenceNumber + SequenceNumber::INITIAL), ChunkNumber(i), false}, watermark});
        maxWatermarkForLastSequence = std::max(maxWatermarkForLastSequence, watermark);
        std::cout << watermark << '\n';
    }
    maxWaterMark[maxSequenceNumber + SequenceNumber::INITIAL] = maxWatermarkForLastSequence;
    inserts.back().first.lastChunk = true;

    /// Shuffle Inserts to create out of order
    std::mt19937 g(rd());
    std::ranges::shuffle(inserts, g);

    ChunkCollector uut;
    std::atomic_size_t completed;

    std::vector<std::jthread> threads;
    threads.reserve(numberOfThreads);
    size_t perThread = inserts.size() / numberOfThreads;
    for (size_t i = 0; i < numberOfThreads; ++i)
    {
        threads.emplace_back(
            [&, i]()
            {
                for (size_t j = 0; j < perThread; ++j)
                {
                    auto [seq, watermark] = inserts[(perThread * i) + j];
                    if (auto opt = uut.collect(seq, Timestamp(watermark)))
                    {
                        ++completed;
                        EXPECT_EQ(opt->first.getRawValue(), seq.sequenceNumber);
                        EXPECT_EQ(opt->second.getRawValue(), maxWaterMark[seq.sequenceNumber]) << seq.sequenceNumber;
                    }
                }
            });
    }

    threads.clear();

    EXPECT_EQ(completed.load(), maxSequenceNumber + SequenceNumber::INITIAL);
}

INSTANTIATE_TEST_SUITE_P(
    ChunkCollectorTest,
    ConcurrentChunkCollectorTest,
    ::testing::Combine(::testing::Values(10, 1000, 50000), ::testing::Values(1, 5, 50), ::testing::Values(1, 4, 16)));

}
