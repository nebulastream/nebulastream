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

#pragma once

#include <cstdint>
#include <optional>
#include <vector>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
// #include <InputFormatters/CircularBuffer.hpp>

namespace NES::InputFormatters::InputFormatterProvider
{

struct StagedTupleBuffer
{
    NES::Memory::TupleBuffer tupleBuffer;
    uint64_t offset = 0;

    [[nodiscard]] std::string_view getStringView() const
    {
        return {tupleBuffer.getBuffer<const char>() + offset, tupleBuffer.getBufferSize() - offset};
    }
};


/*
 * Tasks:           [W1, TB_3], [W3, TB_2], [W2, TB_1]
 * Indexes:             0       1        2        3         4       5
 * Staging Area:    [TB_1.1] [TB_1.2] [TB_2.1] [TB_2.2] [TB_3.1] [TB_3.2]
 * Formula: .1 = 2 * SN - 2 (left shift)
 * Formula: .2 = 2 * SN - 1
 */

/*
 * Tasks:           [W1, TB_3], [W3, TB_2], [W2, TB_1]
 * Indexes:            0            1                2                 3
 * Staging Area:    [EMPTY] [TB_1.2 | TB2.1] [TB_2.2 | TB_3.1] [TB_3.2 | TB_4.1]
 * Formula: .1 = SN
 * Formula: .2 = SN + 1
 */


/*
 * 0   -> 0
 * 1.1 -> 1
 * 1.2 -> 2
 * 2.1 -> 2
 * 2.2 -> 3
 * 3.1 -> 3
 * 3.2 -> 4
 */
class TupleBufferStagingArea
{
    // Todo: lock entire data structure for a solid start
    struct RingBufferIndex
    {
        size_t cycle;
        size_t index;

        // Todo: can use default?
        auto operator<=>(const RingBufferIndex& other) const
        {
            return cycle == other.cycle && index == other.index;
        }
    };

    class SequenceTracker
    {
    private:
        struct SequenceTuple
        {
            SequenceNumber::Underlying low = SequenceNumber::INVALID;
            SequenceNumber::Underlying high = SequenceNumber::INVALID;
            SequenceNumber::Underlying actual = SequenceNumber::INVALID; //could also use variant
        };
    public:
        SequenceTracker(const size_t sizeOfStagingArea) : sequenceTuples(std::vector<SequenceTuple>(sizeOfStagingArea)) { }

        // No need to write any values to other slots than surrounding slots
        // Todo: need to pass RingBufferIndexes, because we need to know the cycles!
        void updatesSequence(RingBufferIndex start, RingBufferIndex end)
        {
            /// assumption: sequence number is behind HEAD (numCycles == head.currentCycles and index >= head.index) or (numCycles == head.currentCycles+1 and index < head.index)
            // Todo: handle index correctly (wrapping)
            auto startIndex = start % sequenceTuples.size();
            auto endIndex = end % sequenceTuples.size();
            const auto startSlotValues = sequenceTuples.at(startIndex);
            const auto endSlotValues = sequenceTuples.at(endIndex);
            // check if start is valid and has lower sequence number
            if (startSlotValues.low != SequenceNumber::INVALID && startSlotValues.low < startIndex) // <--- need to account for wrapping (?) <-- not really, we just count sequence numbers
            {
                // get prior slots before to avoid creating new variable / code duplication
                startIndex = startSlotValues.low;
            }
            // check if end has higher sequence number
            if (endSlotValues.high != SequenceNumber::INVALID && endSlotValues.high > endIndex) // <--- need to account for wrapping (?) <-- not really, we just count sequence numbers
            {
                endIndex = endSlotValues.high;
            }
            // Todo: check if start is head!
            // - if is head, set head to end
            if (startIndex == head.index)
            {
                // Todo: handle cycle
                startIndex = head.index; // set head as startIndex (head always is lowest possible start)
                head.index = endIndex;
            }
            // write updated start to end + 1
            sequenceTuples.at(startIndex - 1).high = endIndex;
            // write updated end to start - 1 (if <= HEAD)
            sequenceTuples.at(endIndex + 1).low = startIndex;

            // Todo: missing wrapping
            // - could also use head as low, enabling to write slot N, if (wrapped) slot N+1 is head
        }

    private:
        RingBufferIndex head;
        std::vector<SequenceTuple> sequenceTuples;
    };

public:
    // Todo: size should be a 'constant' that communicates: a single tuple must never span more tuples than that (<--spilling?)
    // - ring buffer is used for synchronization between tasks, this would then also break down, so it is fine to assume it for sequence numbers
    TupleBufferStagingArea(const size_t sizeOfStagingArea)
        : buffers(std::vector<std::optional<StagedTupleBuffer>>(sizeOfStagingArea)), head(Head{0, sizeOfStagingArea - 1})
    {
    }


    // Todo: how to do cleanup?
    // - take function that moves from vector (works with unique pointers)
    //  - with optionals, it is necessary to set to additionally set to nullopt
    std::optional<StagedTupleBuffer> getPriorStagedTupleBuffer(const SequenceNumber sequenceNumber)
    {
        const size_t index = sequenceNumber.getRawValue() % buffers.size();
        auto buffer = std::move(buffers.at(index));
        buffers.at(index) = std::nullopt;
        return buffer;
    }
    std::optional<StagedTupleBuffer> takeNextStagedTupleBuffer(const SequenceNumber sequenceNumber)
    {
        const size_t index = sequenceNumber.getRawValue() % buffers.size() + 1;
        auto buffer = std::move(buffers.at(index));
        buffers.at(index) = std::nullopt;
        return buffer;
    }

    /// assumption: sequence number is behind HEAD (numCycles == head.currentCycles and index >= head.index) or (numCycles == head.currentCycles+1 and index < head.index)
    // Todo: check if SN is beyond head (head is furthest allowed SN (size of buffers - 1 at the beginning <-- largest allowed index (sequence number)
    bool isInRange(const SequenceNumber sequenceNumber) const
    {
        // Todo: problem: head wrapped and is at 2
        const size_t numCircles = sequenceNumber.getRawValue() / buffers.size();
        const size_t currentPosition = sequenceNumber.getRawValue() % buffers.size();
        return numCircles <= head.numCircles.load()
            && currentPosition
            <= head.currentHead.load(); //Todo: what about nextStagedBuffer <-- does it mean that we need to check with '<'?
    }

    // Todo: determine longest current sequence to move head
    // - not here! must be done by Task/Thread after parsing
    void setPriorTupleBuffer(const NES::SequenceNumber sequenceNumber, NES::Memory::TupleBuffer tupleBuffer, const uint64_t offset)
    {
        /// Can safely set, because we checked 'isInRange' beforehand and index can only go out of range, when slot was used (or it never needed to be used)
        const size_t index = sequenceNumber.getRawValue() % buffers.size();
        buffers[index] = StagedTupleBuffer{.tupleBuffer = std::move(tupleBuffer), .offset = offset};
    }

    void setNextTupleBuffer(const NES::SequenceNumber sequenceNumber, NES::Memory::TupleBuffer tupleBuffer, const uint64_t offset)
    {
        /// Can safely set, because we checked 'isInRange' beforehand and index can only go out of range, when slot was used (or it never needed to be used)
        const size_t index = sequenceNumber.getRawValue() % buffers.size() + 1;
        buffers[index] = StagedTupleBuffer{.tupleBuffer = std::move(tupleBuffer), .offset = offset};
    }

    /// a worker thread tells the staging area that it finished a connected sequence of sequence numbers
    /// example: threads processed sequence numbers 40, 41 and 43, but no thread processed sequence number 42 (or higher) yet ([40,41,X,43])
    /// now, a thread processes the buffer with sequence number 42, connecting the sequence from 40(start) to 43(end) ([40,41,42,43])
    /// if the head was 39 prior, then 43 is now the new head
    /// if the head was smaller than 39, then we access the prior missing sequence number slot and set 43 as the new next biggest possible head
    /// general pattern:
    /// - finishing a sequence (could also be of size 1) either:
    ///     - connects to the current head: [1,2,3] [4,5]: the prior head was 3 and is now 5
    ///     - connects to a separate prior sequence: [X,X] [3] [4,5]: then the slot at '2' should get the information that '5' is the next possible biggest head
    ///         - what if next: [X,X] [3] [4,5] [6,7]: then 2 should get the information that 7 is now the next biggest head
    ///         - todo: how to know what the closest non-processed sequence number is?
    ///     - connects to non-processed sequence number: [X,X,X] [4,5]: like above, case, set value of closest prior non-processed sequence number
    ///         - what if: [X,X,X] [4,5] [6,7]: need to first check if the largest slot [5] had a SN set already
    /// in principle we want a 'function' that takes a sequence number as input and spits out the new head, we want to be able to update that function
    /// with information
    ///
    ///
    /// Idea: atomically update head data structure
    /// - is size of ring buffer (could be part of type of ring buffer elements)
    /// - use index logic to fill todo (but how to detect sequences when wrapping? [1,2,3,4,5,6,1] would be a sequence, if the size is 6)
    /// - initialized with all Xs [X,X,X,X,X,X]
    /// - when adding a sequence, the thread locks the data structure
    /// - it checks if its first slot has a SN in it, if so, it sets its start to that SN
    /// - it checks if its last slot has a SN in it, if so, it sets its end slot to that SN
    /// If start - 1 is the TAIL:
    /// - set TAIL to end
    /// If not:
    /// - it then sets its start at end + 1 (tell Task that processes sequence with next SN where the next prior start of a sequence is (to connect)
    /// - it then sets its end at start - 1 (tell Task that processes sequence with prior SN where the end of the sequence is
    void advanceHead(SequenceNumber start, SequenceNumber end) { }

    // Todo: getPartialTuple is:
    // - check prior/next slot
    //  - if returns nullopt/nullptr, there is no partial tuple (needs to happen synched)
    //    Todo: can we do synching in staging area?
    //      - no, we want to then get a buffer (and do more work)
    //      - yes, could return a buffer, but that would mean the staging area has access to a buffer pool

private:
    // Ringbuffer<StagedTupleBuffer, 4> ringBuffer;
    std::vector<std::optional<StagedTupleBuffer>> buffers;
    std::vector<size_t> sequenceTracker;
    Head head; // Todo make sure that assigning to head is atomic (not like += 1)
};

class InputFormatterOperatorHandler : public NES::Runtime::Execution::OperatorHandler
{
public:
    InputFormatterOperatorHandler() = default;

    /// pipelineExecutionContext, localStateVariableId
    // pipelineExecutionContext
    void start(NES::Runtime::Execution::PipelineExecutionContext&, uint32_t) override { /*noop*/ }
    /// terminationType, pipelineExecutionContext
    void stop(NES::Runtime::QueryTerminationType, NES::Runtime::Execution::PipelineExecutionContext&) override
    {
        /*noop*/
        /// Todo: destroy staging area
    }

    TupleBufferStagingArea& getTupleBufferStagingArea() { return tbStagingArea; }

    bool hasPartialTuple() const { return tbStagingArea.hasPartialTuple(); }

private:
    // Todo: get rid of hardcoded 10
    TupleBufferStagingArea tbStagingArea{10};
};

}