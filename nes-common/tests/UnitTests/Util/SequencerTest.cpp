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

#include <Identifiers/Identifiers.hpp>
#include <Sequencing/SequenceData.hpp>
#include <gtest/gtest.h>

namespace NES
{

TEST(SequencerTest, ChecksOutOnlyOneBufferAndHandlesChunks)
{
    Sequencer<int> sequencer;
    const SequenceData first{INITIAL<SequenceNumber>, INITIAL<ChunkNumber>, false};
    const SequenceData second{INITIAL<SequenceNumber>, ChunkNumber(ChunkNumber::INITIAL + 1), true};
    const SequenceData next{SequenceNumber(SequenceNumber::INITIAL + 1), INITIAL<ChunkNumber>, true};

    EXPECT_EQ(sequencer.take(first, 1), 1);
    EXPECT_FALSE(sequencer.take(second, 2));
    EXPECT_FALSE(sequencer.take(first, 99));
    EXPECT_FALSE(sequencer.take(next, 3));
    EXPECT_EQ(sequencer.acknowledge(first), 2);
    EXPECT_EQ(sequencer.acknowledge(second), 3);
    EXPECT_FALSE(sequencer.acknowledge(next));
}

TEST(SequencerTest, DeferredBufferCanOnlyBeRetriedByTheSamePosition)
{
    Sequencer<int> sequencer;
    const SequenceData first{INITIAL<SequenceNumber>, INITIAL<ChunkNumber>, true};
    const SequenceData next{SequenceNumber(SequenceNumber::INITIAL + 1), INITIAL<ChunkNumber>, true};

    EXPECT_EQ(sequencer.take(first, 1), 1);
    sequencer.defer(first);
    EXPECT_FALSE(sequencer.take(next, 2));
    EXPECT_EQ(sequencer.take(first, 1), 1);
    EXPECT_EQ(sequencer.acknowledge(first), 2);
}

TEST(DropOutOfOrderSequencerTest, DropsOlderArrivalsAfterAcknowledgement)
{
    DropOutOfOrderSequencer<int> sequencer;
    const SequenceData one{SequenceNumber(1), INITIAL<ChunkNumber>, true};
    const SequenceData two{SequenceNumber(2), INITIAL<ChunkNumber>, true};
    const SequenceData three{SequenceNumber(3), INITIAL<ChunkNumber>, true};

    EXPECT_EQ(sequencer.take(one, 1), 1);
    EXPECT_FALSE(sequencer.take(three, 3));
    EXPECT_FALSE(sequencer.take(two, 2));
    EXPECT_EQ(sequencer.acknowledge(one), 3);
    EXPECT_FALSE(sequencer.acknowledge(three));
}

TEST(DropOutOfOrderSequencerTest, LastChunkClosesItsSequence)
{
    DropOutOfOrderSequencer<int> sequencer;
    const SequenceData last{SequenceNumber(5), ChunkNumber(2), true};
    const SequenceData invalidLaterChunk{SequenceNumber(5), ChunkNumber(3), false};
    const SequenceData nextSequence{SequenceNumber(6), INITIAL<ChunkNumber>, true};

    EXPECT_EQ(sequencer.take(last, 1), 1);
    EXPECT_FALSE(sequencer.acknowledge(last));
    EXPECT_FALSE(sequencer.take(invalidLaterChunk, 2));
    EXPECT_EQ(sequencer.take(nextSequence, 3), 3);
}

}
