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

#include <InputFormatter.hpp>

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <ostream>
#include <span>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypesUtil.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <RawBufferIndex.hpp>
#include <RawTupleBuffer.hpp>
#include <SequenceShredder.hpp>
#include <function.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES
{

/// Standard-layout view that traced nautilus code walks via offsetof. Holds non-owning RawBufferIndex pointers; ownership lives in
/// the sibling 'tlOwnedRawBufferIndices'. Splitting layout from ownership keeps this struct standard-layout so the RawBufferIndex pointers
/// sit at fixed, traceable offsets that getMemberWithOffset can read directly.
struct IndexPhaseResult
{
    std::span<char> leadingSpanningTuple;
    std::span<char> trailingSpanningTuple;
    bool hasLeadingSpanningTupleBool = false;
    bool hasTupleDelimiter = false;
    bool isRepeat = false;
    /// Stashed from the indexer's RawBufferIndex; read later by 'indexTrailingSpanningTupleProxy' instead of dereferencing the RawBufferIndex.
    FieldIndex offsetOfLastTupleInRawBuffer = std::numeric_limits<FieldIndex>::max();

    RawBufferIndex* leadingSpanningTupleRawBufferIndex = nullptr;
    RawBufferIndex* trailingSpanningTupleRawBufferIndex = nullptr;
    RawBufferIndex* rawBufferIndex = nullptr;
};

static_assert(std::is_standard_layout_v<IndexPhaseResult>, "IndexPhaseResult must have a standard layout for safe usage of offsetof");

namespace
{

/// Constructs a spanning tuple (string) that spans over at least two buffers (stagedBuffersSpan).
/// First, determines the start of the spanning tuple in the first buffer to format. Constructs a spanning tuple from the required bytes.
/// Second, appends all bytes of all raw buffers that are not the last buffer to the spanning tuple.
/// Third, determines the end of the spanning tuple in the last buffer to format. Appends the required bytes to the spanning tuple.
/// Lastly, formats the full spanning tuple.
void processSpanningTuple(
    const std::span<const StagedBuffer>& stagedBuffersSpan, std::span<char> spanningTupleBuffer, const InputFormatIndexer& indexer)
{
    INVARIANT(stagedBuffersSpan.size() >= 2, "A spanning tuple must span across at least two buffers");

    size_t offset = 0;
    auto boundsCheckedCopy = [&spanningTupleBuffer, &offset](std::string_view source)
    {
        INVARIANT(offset + source.size() <= spanningTupleBuffer.size(), "Buffer overflow: insufficient space in spanning tuple buffer");

        auto destSubspan = spanningTupleBuffer.subspan(offset, source.size());
        std::ranges::copy(source, destSubspan.begin());
        offset += source.size();
    };

    /// Wrap the spanning tuple in delimiter bytes (if exist) and copy the trailing bytes of the first buffer into the spanning tuple
    boundsCheckedCopy(indexer.getTupleDelimitingBytes());
    boundsCheckedCopy(stagedBuffersSpan.front().getTrailingBytes(indexer.getTupleDelimitingBytes().size()));

    /// Copy all complete buffers between the first and the last into the spanning tuple
    for (const auto& buffer : stagedBuffersSpan.subspan(1, stagedBuffersSpan.size() - 2))
    {
        boundsCheckedCopy(buffer.getBufferView());
    }

    /// Copy the leading bytes of the last buffer into the spanning tuple and wrap the spanning tuple with delimiting bytes (if exist)
    boundsCheckedCopy(stagedBuffersSpan.back().getLeadingBytes());
    boundsCheckedCopy(indexer.getTupleDelimitingBytes());
}

struct OwnedRawBufferIndices
{
    std::unique_ptr<RawBufferIndex> leadingSpanningTupleRawBufferIndex;
    std::unique_ptr<RawBufferIndex> trailingSpanningTupleRawBufferIndex;
    std::unique_ptr<RawBufferIndex> rawBufferIndex;
};

/// We need thread_local variables to 'bridge' state between the indexing (open) and the parsing (readBuffer) phase.
/// They must be thread_local, since multiple threads use them at the same time.
/// 'thread_local' means that every thread creates its own 'local' static version.
thread_local IndexPhaseResult tlIndexPhaseResult{};
thread_local OwnedRawBufferIndices tlOwnedRawBufferIndices{};

IndexPhaseResult* getIndexPhaseResult()
{
    return &tlIndexPhaseResult;
}

void assignLeadingRawBufferIndex(std::unique_ptr<RawBufferIndex> rawBufferIndex)
{
    tlIndexPhaseResult.leadingSpanningTupleRawBufferIndex = rawBufferIndex.get();
    tlOwnedRawBufferIndices.leadingSpanningTupleRawBufferIndex = std::move(rawBufferIndex);
}

void assignTrailingRawBufferIndex(std::unique_ptr<RawBufferIndex> rawBufferIndex)
{
    tlIndexPhaseResult.trailingSpanningTupleRawBufferIndex = rawBufferIndex.get();
    tlOwnedRawBufferIndices.trailingSpanningTupleRawBufferIndex = std::move(rawBufferIndex);
}

void assignRawBufferIndex(std::unique_ptr<RawBufferIndex> rawBufferIndex)
{
    tlIndexPhaseResult.rawBufferIndex = rawBufferIndex.get();
    tlOwnedRawBufferIndices.rawBufferIndex = std::move(rawBufferIndex);
}

/// Default constructs the indexer's RawBufferIndices to allow tracing the correct 'readSpanningRecord' function. Always replaces —
/// tlOwnedRawBufferIndices is thread_local and persists across InputFormatters on the same thread, so any prior RawBufferIndex might be of a
/// different derived type than the current indexer. Tracing through a stale RawBufferIndex would dispatch to the wrong derived
/// readSpanningRecord and cast the indexer to the wrong type.
void setDefaultRawBufferIndicesForTracing(const InputFormatIndexer& indexer)
{
    assignLeadingRawBufferIndex(indexer.indexRawBuffer({}));
    assignTrailingRawBufferIndex(indexer.indexRawBuffer({}));
    assignRawBufferIndex(indexer.indexRawBuffer({}));
}

/// Builds an index during the indexing phase (open()), which the parsing phase (readBuffer()) requires for accessing specific fields.
/// Does not have state itself, but wraps the static thread_local 'tlIndexPhaseResult', which it builds during its lifetime.
class IndexPhaseResultBuilder
{
public:
    IndexPhaseResultBuilder() = delete;

    struct IndexResult
    {
        FieldIndex offsetOfFirstTupleDelimiter;
        FieldIndex offsetOfLastTupleDelimiter;
        bool hasTupleDelimiter;
    };

    static void startBuildingIndex()
    {
        tlIndexPhaseResult = IndexPhaseResult{};
        tlOwnedRawBufferIndices = OwnedRawBufferIndices{};
    }

    static IndexResult indexRawBuffer(InputFormatIndexer& indexer, const TupleBuffer& tupleBuffer)
    {
        auto result = indexer.indexRawBuffer({tupleBuffer.getAvailableMemoryArea<char>().data(), tupleBuffer.getNumberOfTuples()});
        const auto [offsetOfFirstTupleDelimiter, offsetOfLastTupleDelimiter] = result->getTupleDelimiterOffsets();
        assignRawBufferIndex(std::move(result));

        tlIndexPhaseResult.offsetOfLastTupleInRawBuffer = offsetOfLastTupleDelimiter;

        tlIndexPhaseResult.hasTupleDelimiter = offsetOfFirstTupleDelimiter < tupleBuffer.getBufferSize();
        return {
            .offsetOfFirstTupleDelimiter = offsetOfFirstTupleDelimiter,
            .offsetOfLastTupleDelimiter = offsetOfLastTupleDelimiter,
            .hasTupleDelimiter = tlIndexPhaseResult.hasTupleDelimiter};
    }

    static void setIsRepeat(bool isRepeat) { tlIndexPhaseResult.isRepeat = isRepeat; }

    static IndexPhaseResult* finalizeLeadingIndexPhase() { return &tlIndexPhaseResult; }

    static void constructAndIndexTrailingSpanningTuple(
        const std::vector<StagedBuffer>& stagedBuffers,
        InputFormatIndexer& indexer,
        Arena& arenaRef,
        const size_t sizeOfTrailingSpanningTuple)
    {
        allocateForTrailingSpanningTuple(arenaRef, sizeOfTrailingSpanningTuple);
        const auto trailingSpanningTupleBuffers = std::span(stagedBuffers);
        processSpanningTuple(trailingSpanningTupleBuffers, tlIndexPhaseResult.trailingSpanningTuple, indexer);
        assignTrailingRawBufferIndex(indexer.indexRawBuffer(
            {std::bit_cast<const char*>(tlIndexPhaseResult.trailingSpanningTuple.data()),
             tlIndexPhaseResult.trailingSpanningTuple.size()}));
    }

    static void constructAndIndexLeadingSpanningTuple(
        const std::vector<StagedBuffer>& leadingSpanningTupleBuffers, InputFormatIndexer& indexer, Arena& arenaRef)
    {
        const auto sizeOfLeadingSpanningTuple
            = calculateSizeOfSpanningTuple(leadingSpanningTupleBuffers, indexer.getTupleDelimitingBytes().size());
        if (sizeOfLeadingSpanningTuple <= indexer.getTupleDelimitingBytes().size() * 2)
        {
            return;
        }
        allocateForLeadingSpanningTuple(arenaRef, sizeOfLeadingSpanningTuple);
        processSpanningTuple(leadingSpanningTupleBuffers, tlIndexPhaseResult.leadingSpanningTuple, indexer);
        assignLeadingRawBufferIndex(indexer.indexRawBuffer(
            {std::bit_cast<const char*>(tlIndexPhaseResult.leadingSpanningTuple.data()), tlIndexPhaseResult.leadingSpanningTuple.size()}));
    }

    static size_t
    calculateSizeOfSpanningTuple(const std::span<const StagedBuffer> spanningTupleBuffers, const size_t sizeOfTupleDelimitingBytes)
    {
        size_t sizeOfSpanningTuple = 0;
        sizeOfSpanningTuple += (2 * sizeOfTupleDelimitingBytes);
        sizeOfSpanningTuple += spanningTupleBuffers.front().getTrailingBytes(sizeOfTupleDelimitingBytes).size();
        for (size_t i = 1; i < spanningTupleBuffers.size() - 1; ++i)
        {
            sizeOfSpanningTuple += spanningTupleBuffers[i].getSizeOfBufferInBytes();
        }
        sizeOfSpanningTuple += spanningTupleBuffers.back().getLeadingBytes().size();
        return sizeOfSpanningTuple;
    }

private:
    static void allocateForLeadingSpanningTuple(Arena& arenaRef, const size_t sizeOfLeadingSpanningTuple)
    {
        tlIndexPhaseResult.hasLeadingSpanningTupleBool = true;
        auto byteSpan = arenaRef.allocateMemory(sizeOfLeadingSpanningTuple);
        tlIndexPhaseResult.leadingSpanningTuple = std::span<char>(std::bit_cast<char*>(byteSpan.data()), byteSpan.size());
    }

    static void allocateForTrailingSpanningTuple(Arena& arenaRef, const size_t sizeOfTrailingSpanningTuple)
    {
        auto byteSpan = arenaRef.allocateMemory(sizeOfTrailingSpanningTuple);
        tlIndexPhaseResult.trailingSpanningTuple = std::span<char>(std::bit_cast<char*>(byteSpan.data()), byteSpan.size());
    }
};

bool indexTrailingSpanningTupleProxy(
    const TupleBuffer* tupleBuffer, Arena* arenaRef, InputFormatIndexer* indexer, SequenceShredder* sequenceShredder)
{
    /// Value was stashed on tlIndexPhaseResult when the indexer returned its RawBufferIndex; max() means no delimiter in the buffer.
    const auto offsetOfLastTupleDelimiter = tlIndexPhaseResult.offsetOfLastTupleInRawBuffer;
    if (offsetOfLastTupleDelimiter == std::numeric_limits<FieldIndex>::max())
    {
        return false;
    }

    auto stagedBuffers = sequenceShredder->findTrailingSpanningTupleWithDelimiter(tupleBuffer->getSequenceNumber());
    /// a trailing spanning tuple must span at least 2 buffers
    if (stagedBuffers.getSize() < 2)
    {
        return false;
    }
    const auto sizeOfTrailingSpanningTuple = IndexPhaseResultBuilder::calculateSizeOfSpanningTuple(
        stagedBuffers.getSpanningBuffers(), indexer->getTupleDelimitingBytes().size());
    /// If the spanning tuple consists only of delimiters, it is empty and we can skip processing it
    if (sizeOfTrailingSpanningTuple <= indexer->getTupleDelimitingBytes().size() * 2)
    {
        return false;
    }
    IndexPhaseResultBuilder::constructAndIndexTrailingSpanningTuple(
        stagedBuffers.getSpanningBuffers(), *indexer, *arenaRef, sizeOfTrailingSpanningTuple);
    return true;
}

IndexPhaseResult* indexLeadingSpanningTupleAndBufferProxy(
    const TupleBuffer* tupleBuffer, Arena* arenaRef, InputFormatIndexer* indexer, SequenceShredder* sequenceShredder)
{
    IndexPhaseResultBuilder::startBuildingIndex();
    const auto [offsetOfFirstTupleDelimiter, offsetOfLastTupleDelimiter, hasTupleDelimiter]
        = IndexPhaseResultBuilder::indexRawBuffer(*indexer, *tupleBuffer);

    if (hasTupleDelimiter)
    {
        const auto [isInRange, stagedBuffers] = sequenceShredder->findLeadingSpanningTupleWithDelimiter(
            StagedBuffer{RawTupleBuffer{*tupleBuffer}, offsetOfFirstTupleDelimiter, offsetOfLastTupleDelimiter});
        if (not isInRange)
        {
            IndexPhaseResultBuilder::setIsRepeat(true);
            return IndexPhaseResultBuilder::finalizeLeadingIndexPhase();
        }
        if (stagedBuffers.getSize() < 2)
        {
            return IndexPhaseResultBuilder::finalizeLeadingIndexPhase();
        }
        IndexPhaseResultBuilder::constructAndIndexLeadingSpanningTuple(stagedBuffers.getSpanningBuffers(), *indexer, *arenaRef);
    }
    else /* has no tuple delimiter */
    {
        const auto [isInRange, stagedBuffers] = sequenceShredder->findSpanningTupleWithoutDelimiter(
            StagedBuffer{RawTupleBuffer{*tupleBuffer}, offsetOfFirstTupleDelimiter, offsetOfFirstTupleDelimiter});

        if (not isInRange)
        {
            IndexPhaseResultBuilder::setIsRepeat(true);
            return IndexPhaseResultBuilder::finalizeLeadingIndexPhase();
        }

        if (stagedBuffers.getSize() < 3)
        {
            return IndexPhaseResultBuilder::finalizeLeadingIndexPhase();
        }

        /// The buffer has no delimiter, but connects two buffers with delimiters, forming one spanning tuple
        /// We arbitrarily treat it as a 'leading' spanning tuple (technically, it is both leading and trailing)
        IndexPhaseResultBuilder::constructAndIndexLeadingSpanningTuple(stagedBuffers.getSpanningBuffers(), *indexer, *arenaRef);
    }
    return IndexPhaseResultBuilder::finalizeLeadingIndexPhase();
}

void parseLeadingRecord(
    ExecutionContext& executionCtx,
    const std::function<void(ExecutionContext& executionCtx, Record& record)>& executeChild,
    const nautilus::val<IndexPhaseResult*>& indexPhaseResult,
    const std::vector<Record::RecordFieldIdentifier>& projections,
    const InputFormatIndexer& indexer,
    const TupleBufferRef& bufferRef)
{
    if (*getMemberWithOffset<bool>(indexPhaseResult, offsetof(IndexPhaseResult, hasLeadingSpanningTupleBool)))
    {
        /// Get leading RawBufferIndex and a pointer to the spanning tuple 'record'
        auto spanningRecordPtr = *getMemberPtrWithOffset<int8_t>(indexPhaseResult, offsetof(IndexPhaseResult, leadingSpanningTuple));
        auto leadingRawBufferIndex
            = *getMemberWithOffset<RawBufferIndex*>(indexPhaseResult, offsetof(IndexPhaseResult, leadingSpanningTupleRawBufferIndex));

        auto record = getIndexPhaseResult()->leadingSpanningTupleRawBufferIndex->readSpanningRecord(
            projections, spanningRecordPtr, nautilus::val<uint64_t>(0), indexer, leadingRawBufferIndex, bufferRef);
        executeChild(executionCtx, record);
    }
}

void parseRecordsInRawBuffer(
    ExecutionContext& executionCtx,
    const RecordBuffer& recordBuffer,
    const std::function<void(ExecutionContext& executionCtx, Record& record)>& executeChild,
    const nautilus::val<IndexPhaseResult*>& indexPhaseResult,
    const std::vector<Record::RecordFieldIdentifier>& projections,
    const InputFormatIndexer& indexer,
    const TupleBufferRef& bufferRef)
{
    nautilus::val<uint64_t> bufferRecordIdx = 0;
    auto rawBufferIndexVal = *getMemberWithOffset<RawBufferIndex*>(indexPhaseResult, offsetof(IndexPhaseResult, rawBufferIndex));
    while (getIndexPhaseResult()->rawBufferIndex->hasNext(bufferRecordIdx, rawBufferIndexVal))
    {
        auto record = getIndexPhaseResult()->rawBufferIndex->readSpanningRecord(
            projections, recordBuffer.getMemArea(), bufferRecordIdx, indexer, rawBufferIndexVal, bufferRef);
        executeChild(executionCtx, record);
        bufferRecordIdx += 1;
    }
}

void parseTrailingRecord(
    ExecutionContext& executionCtx,
    const RecordBuffer& recordBuffer,
    const std::function<void(ExecutionContext& executionCtx, Record& record)>& executeChild,
    const nautilus::val<IndexPhaseResult*>& indexPhaseResult,
    const std::vector<Record::RecordFieldIdentifier>& projections,
    InputFormatIndexer& indexer,
    SequenceShredder& sequenceShredder,
    const TupleBufferRef& bufferRef)
{
    const nautilus::val<bool> hasTrailingSpanningTuple = invoke(
        indexTrailingSpanningTupleProxy,
        recordBuffer.getReference(),
        executionCtx.pipelineMemoryProvider.arena.getArena(),
        nautilus::val<InputFormatIndexer*>(&indexer),
        nautilus::val<SequenceShredder*>(&sequenceShredder));

    if (hasTrailingSpanningTuple)
    {
        auto spanningRecordPtr = *getMemberPtrWithOffset<int8_t>(indexPhaseResult, offsetof(IndexPhaseResult, trailingSpanningTuple));
        auto trailingRawBufferIndex
            = *getMemberWithOffset<RawBufferIndex*>(indexPhaseResult, offsetof(IndexPhaseResult, trailingSpanningTupleRawBufferIndex));

        auto record = getIndexPhaseResult()->trailingSpanningTupleRawBufferIndex->readSpanningRecord(
            projections, spanningRecordPtr, nautilus::val<uint64_t>(0), indexer, trailingRawBufferIndex, bufferRef);
        executeChild(executionCtx, record);
    }
}

}

std::vector<DataType> InputFormatter::getAllDataTypes() const
{
    INVARIANT(false, "unsupported operation on InputFormatter");
    std::unreachable();
}

Record InputFormatter::readRecord(const std::vector<Record::RecordFieldIdentifier>&, const RecordBuffer&, nautilus::val<uint64_t>&) const
{
    INVARIANT(false, "Does not implement 'readRecord()'");
    std::unreachable();
}

TupleBufferRef::WriteRecordResult InputFormatter::writeRecord(
    nautilus::val<uint64_t>&, const RecordBuffer&, const Record&, const nautilus::val<AbstractBufferProvider*>&) const
{
    INVARIANT(false, "unsupported operation on InputFormatter");
    std::unreachable();
}

std::vector<Record::RecordFieldIdentifier> InputFormatter::getAllFieldNames() const
{
    INVARIANT(false, "unsupported operation on InputFormatter");
    std::unreachable();
}

nautilus::val<bool> InputFormatter::indexBuffer(const RecordBuffer& recordBuffer, const ArenaRef& arenaRef) const
{
    setDefaultRawBufferIndicesForTracing(*this->inputFormatIndexer);
    /// index raw tuple buffer, resolve and index spanning tuples(SequenceShredder) and return pointers to resolved spanning tuples, if exist
    const auto tlIndexPhaseResultNautilusVal = std::make_unique<nautilus::val<IndexPhaseResult*>>(invoke(
        indexLeadingSpanningTupleAndBufferProxy,
        recordBuffer.getReference(),
        arenaRef.getArena(),
        nautilus::val<InputFormatIndexer*>(this->inputFormatIndexer.get()),
        nautilus::val<SequenceShredder*>(this->sequenceShredder.get())));

    if (/* isRepeat */ *getMemberWithOffset<bool>(*tlIndexPhaseResultNautilusVal, offsetof(IndexPhaseResult, isRepeat)))
    {
        return {false};
    }

    return {true};
}

void InputFormatter::readBuffer(
    ExecutionContext& executionCtx,
    const RecordBuffer& recordBuffer,
    const std::function<void(ExecutionContext& executionCtx, Record& record)>& executeChild)
{
    /// @Note: the order below is important
    const nautilus::val<IndexPhaseResult*> indexPhaseResult = nautilus::invoke(getIndexPhaseResult);

    /// a buffer that only contains data from a single tuple may connect two buffers that delimit tuples
    /// we count such a spanning tuple as a leading spanning tuple
    /// a buffer that delimits tuples may form a leading (and a trailing) spanning tuple
    parseLeadingRecord(executionCtx, executeChild, indexPhaseResult, this->projections, *this->inputFormatIndexer, *this->memoryProvider);

    /// check if the buffer only contains data from a single tuple (does not delimit two tuples)
    /// such a buffer can only form one (leading) spanning tuple, so returning is safe
    if (not(nautilus::val<bool>(*getMemberWithOffset<bool>(indexPhaseResult, offsetof(IndexPhaseResult, hasTupleDelimiter)))))
    {
        return;
    }

    /// a buffer that delimits tuples may contain multiple complete tuples
    /// determining the offset of a tuple may require parsing the prior tuple
    parseRecordsInRawBuffer(
        executionCtx, recordBuffer, executeChild, indexPhaseResult, this->projections, *this->inputFormatIndexer, *this->memoryProvider);

    /// a buffer that delimits tuples usually forms a spanning tuple that continues in the next buffer
    /// determining the offset of the start of that tuple may require parsing all prior records in the raw buffer
    parseTrailingRecord(
        executionCtx,
        recordBuffer,
        executeChild,
        indexPhaseResult,
        this->projections,
        *this->inputFormatIndexer,
        *this->sequenceShredder,
        *this->memoryProvider);
}

std::ostream& InputFormatter::toString(std::ostream& os) const
{
    /// Not using fmt::format, because it fails during build, trying to pass sequenceShredder as a const value
    os << "InputFormatter(inputFormatIndexer: " << *inputFormatIndexer << ", sequenceShredder: " << *sequenceShredder << ")\n";
    return os;
}

}
