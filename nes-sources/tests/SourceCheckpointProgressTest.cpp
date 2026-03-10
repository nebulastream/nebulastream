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

#include <Sources/SourceCheckpointProgress.hpp>

#include <Identifiers/Identifiers.hpp>
#include <gtest/gtest.h>

namespace NES
{

TEST(SourceCheckpointProgressTest, StartsAtInvalidSequence)
{
    SourceCheckpointProgress progress;

    EXPECT_EQ(progress.getHighestContiguousCompletedSequence(), INVALID_SEQ_NUMBER.getRawValue());
}

TEST(SourceCheckpointProgressTest, AdvancesOnlyForContiguousSequences)
{
    SourceCheckpointProgress progress;

    progress.noteCompletedSequence(SequenceNumber(2));
    EXPECT_EQ(progress.getHighestContiguousCompletedSequence(), INVALID_SEQ_NUMBER.getRawValue());

    progress.noteCompletedSequence(SequenceNumber(1));
    EXPECT_EQ(progress.getHighestContiguousCompletedSequence(), SequenceNumber(2).getRawValue());

    progress.noteCompletedSequence(SequenceNumber(4));
    EXPECT_EQ(progress.getHighestContiguousCompletedSequence(), SequenceNumber(2).getRawValue());

    progress.noteCompletedSequence(SequenceNumber(3));
    EXPECT_EQ(progress.getHighestContiguousCompletedSequence(), SequenceNumber(4).getRawValue());
}

TEST(SourceCheckpointProgressTest, ResetDropsBufferedCompletions)
{
    SourceCheckpointProgress progress;

    progress.noteCompletedSequence(SequenceNumber(1));
    progress.noteCompletedSequence(SequenceNumber(2));
    EXPECT_EQ(progress.getHighestContiguousCompletedSequence(), SequenceNumber(2).getRawValue());

    progress.reset();
    EXPECT_EQ(progress.getHighestContiguousCompletedSequence(), INVALID_SEQ_NUMBER.getRawValue());

    progress.noteCompletedSequence(SequenceNumber(2));
    EXPECT_EQ(progress.getHighestContiguousCompletedSequence(), INVALID_SEQ_NUMBER.getRawValue());

    progress.noteCompletedSequence(SequenceNumber(1));
    EXPECT_EQ(progress.getHighestContiguousCompletedSequence(), SequenceNumber(2).getRawValue());
}

}
