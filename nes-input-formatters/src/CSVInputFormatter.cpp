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

#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iostream>
#include <limits>
#include <memory>
#include <ostream>
#include <ranges>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>
#include <strings.h>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <InputFormatters/InputFormatter.hpp>
#include <InputFormatters/InputFormatterTask.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <boost/token_functions.hpp>
#include <boost/tokenizer.hpp>
#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>
#include <CSVInputFormatter.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatterRegistry.hpp>
#include <PipelineExecutionContext.hpp>
#include <SequenceShredder.hpp>

namespace NES::InputFormatters
{

struct PartialTuple
{
    uint64_t offsetInBuffer;
    Memory::TupleBuffer rawTB;

    [[nodiscard]] std::string_view getStringView() const
    {
        return {rawTB.getBuffer<const char>() + offsetInBuffer, rawTB.getBufferSize() - offsetInBuffer};
    }
};

class ProgressTracker
{
public:
    ProgressTracker(
        SequenceNumber sequenceNumber,
        const OriginId originId,
        std::string tupleDelimiter,
        const size_t tupleSizeInBytes,
        const size_t numberOfSchemaFields)
        : sequenceNumber(sequenceNumber)
        , chunkNumber(ChunkNumber(1))
        , originId(originId)
        , tupleDelimiter(std::move(tupleDelimiter))
        , tupleSizeInBytes(tupleSizeInBytes)
        , numSchemaFields(numberOfSchemaFields) { };

    ~ProgressTracker() = default;

    void resetForNewRawTB(
        const size_t sizeOfNewTupleBufferRawInBytes, const char* newTupleBufferRaw, const size_t startOffset, const size_t endOffset)
    {
        this->numTotalBytesInrawTB = sizeOfNewTupleBufferRawInBytes;
        this->currentTupleStartrawTB = startOffset;
        this->endOfTuplesInRawTB = (endOffset == 0) ? sizeOfNewTupleBufferRawInBytes : endOffset + tupleSizeInBytes;
        this->tupleBufferRawSV = std::string_view(newTupleBufferRaw, numTotalBytesInrawTB);
        this->currentTupleEndrawTB = findIndexOfNextTuple();
    }

    void resetForNewRawTB(
        const size_t sizeOfNewTupleBufferRawInBytes,
        const char* newTupleBufferRaw,
        const size_t startOffset,
        const size_t endOffset,
        const size_t currentFieldOffsetTBFormatted)
    {
        this->numTotalBytesInrawTB = sizeOfNewTupleBufferRawInBytes;
        this->currentTupleStartrawTB = startOffset;
        this->endOfTuplesInRawTB = (endOffset == 0) ? sizeOfNewTupleBufferRawInBytes : endOffset + tupleSizeInBytes;
        this->currentFieldOffsetTBFormatted = currentFieldOffsetTBFormatted;
        this->tupleBufferRawSV = std::string_view(newTupleBufferRaw, numTotalBytesInrawTB);
        this->currentTupleEndrawTB = findIndexOfNextTuple();
        this->numTuplesInTBFormatted = 0;
    }

    int8_t* getCurrentFieldPointer() { return (this->tupleBufferFormatted.getBuffer() + currentFieldOffsetTBFormatted); }

    [[nodiscard]] size_t getOffsetOfFirstTupleDelimiter() const { return currentTupleEndrawTB; }
    [[nodiscard]] size_t getOffsetOfLastTupleDelimiter() const
    {
        return tupleBufferRawSV.rfind(tupleDelimiter, tupleBufferRawSV.size() - 1);
    }


    void processCurrentTuple(
        std::string_view currentTuple,
        const std::string& fieldDelimiter,
        const std::vector<CSVInputFormatter::CastFunctionSignature>& fieldParseFunctions,
        const std::vector<size_t>& fieldSizes,
        Memory::AbstractBufferProvider& bufferProvider)
    {
        const boost::char_separator sep{fieldDelimiter.c_str()};
        const boost::tokenizer tupleTokenizer{currentTuple.begin(), currentTuple.end(), sep};
        /// Iterate over all fields, parse the string values and write the formatted data into the TBF.
        for (auto [token, parseFunction, fieldSize] : std::views::zip(tupleTokenizer, fieldParseFunctions, fieldSizes))
        {
            const auto fieldPointer = this->tupleBufferFormatted.getBuffer() + currentFieldOffsetTBFormatted;
            parseFunction(token, fieldPointer, bufferProvider, getTupleBufferFormatted());
            currentFieldOffsetTBFormatted += fieldSize;
        }
    }

    void checkAndHandleFullBuffer(PipelineExecutionContext& pipelineExecutionContext)
    {
        const auto availableBytes = tupleBufferFormatted.getBufferSize() - currentFieldOffsetTBFormatted;
        if (const auto isFull = availableBytes < tupleSizeInBytes)
        {
            /// Emit TBF and get new TBF
            setNumberOfTuplesInTBFormatted();
            NES_TRACE("emitting TupleBuffer with {} tuples.", numTuplesInTBFormatted);

            /// As we are not done with the current sequence number, we need to emit the TBF with the lastChunk flag set to false.
            auto tbf = getTupleBufferFormatted();
            tbf.setLastChunk(false);
            pipelineExecutionContext.emitBuffer(tbf, NES::PipelineExecutionContext::ContinuationPolicy::POSSIBLE);


            /// We need to increment the chunk number as we are not done with the current sequence number.
            chunkNumber = ChunkNumber(chunkNumber.getRawValue() + 1);
            setNewTupleBufferFormatted(pipelineExecutionContext.allocateTupleBuffer());
            currentFieldOffsetTBFormatted = 0;
            numTuplesInTBFormatted = 0;
        }
    }

    void processCurrentBuffer(
        PipelineExecutionContext& pipelineExecutionContext,
        const std::string& fieldDelimiter,
        const std::vector<CSVInputFormatter::CastFunctionSignature>& fieldParseFunctions,
        const std::vector<size_t>& fieldSizes)
    {
        auto& bufferProvider = *pipelineExecutionContext.getBufferManager();

        while (hasOneMoreTupleInRawTB())
        {
            /// If we parsed a (prior) partial tuple before, the TBF may not fit another tuple. We emit the single (prior) partial tuple.
            /// Otherwise, the TBF is empty. Since we assume that a tuple is never bigger than a TBF, we can fit at least one more tuple.
            checkAndHandleFullBuffer(pipelineExecutionContext);

            /// Get next tuple
            const auto currentTuple = getNextTuple();
            /// Parse fields of tuple
            processCurrentTuple(currentTuple, fieldDelimiter, fieldParseFunctions, fieldSizes, bufferProvider);

            /// Progress one tuple
            progressOneTuple();
        }
    }
    [[nodiscard]] bool hasOneMoreTupleInRawTB() const { return currentTupleEndrawTB != std::numeric_limits<uint64_t>::max(); }

    [[nodiscard]] size_t sizeOfCurrentTuple() const { return this->currentTupleEndrawTB - this->currentTupleStartrawTB; }

    void progressOneTuple()
    {
        ++numTuplesInTBFormatted;
        currentTupleStartrawTB = currentTupleEndrawTB + 1;
        currentTupleEndrawTB = findIndexOfNextTuple();
    }

    [[nodiscard]] std::string_view getInitialPartialTuple(const size_t sizeOfPartialTuple) const
    {
        return this->tupleBufferRawSV.substr(0, sizeOfPartialTuple);
    }

    [[nodiscard]] std::string_view getNextTuple() const
    {
        return this->tupleBufferRawSV.substr(currentTupleStartrawTB, sizeOfCurrentTuple());
    }

    void setNewTupleBufferFormatted(NES::Memory::TupleBuffer&& tupleBufferFormatted)
    {
        tupleBufferFormatted.setSequenceNumber(sequenceNumber);
        tupleBufferFormatted.setChunkNumber(chunkNumber);
        tupleBufferFormatted.setOriginId(originId);
        this->tupleBufferFormatted = std::move(tupleBufferFormatted);
    }

    std::string handlePriorPartialTuple()
    {
        std::string partialTuple;
        if (not(this->partialTuple.empty()))
        {
            partialTuple.resize(this->partialTuple.size() + currentTupleEndrawTB);
            partialTuple = std::string(this->partialTuple) + std::string(tupleBufferRawSV.substr(0, currentTupleEndrawTB));
        }
        return partialTuple;
    }

    std::string_view getEndingPartialTuple() const { return tupleBufferRawSV.substr(currentTupleStartrawTB, numTotalBytesInrawTB); }

    friend std::ostream& operator<<(std::ostream& out, const ProgressTracker& progressTracker)
    {
        return out << fmt::format(
                   "tupleDelimiter: {}, size of tuples in bytes: {}, number of fields in schema: {}, rawTB(total number of bytes: {}, "
                   "start of "
                   "current tuple: {}, end of current tuple: {}), tbFormatted(number of tuples: {}, current field offset: {})",
                   (progressTracker.tupleDelimiter == "\n") ? "\\n" : progressTracker.tupleDelimiter,
                   progressTracker.tupleSizeInBytes,
                   progressTracker.numSchemaFields,
                   progressTracker.numTotalBytesInrawTB,
                   progressTracker.currentTupleStartrawTB,
                   progressTracker.currentTupleEndrawTB,
                   progressTracker.numTuplesInTBFormatted,
                   progressTracker.currentFieldOffsetTBFormatted);
    }

    /// Getter & Setter
    [[nodiscard]] uint64_t getNumSchemaFields() const { return this->numSchemaFields; }
    NES::Memory::TupleBuffer& getTupleBufferFormatted() { return this->tupleBufferFormatted; }
    void setCurrentTupleStartrawTB(const size_t newCurrentTupleStartrawTB) { this->currentTupleStartrawTB = newCurrentTupleStartrawTB; }
    void setNumberOfTuplesInTBFormatted()
    {
        this->tupleBufferFormatted.setNumberOfTuples(numTuplesInTBFormatted);
        this->tupleBufferFormatted.setUsedMemorySize(numTuplesInTBFormatted * tupleSizeInBytes);
    }
    uint64_t getNumTuplesInTBFormatted() { return numTuplesInTBFormatted; }
    const std::string& getTupleDelimiter() { return this->tupleDelimiter; }

    size_t currentTupleStartrawTB{0};
    size_t endOfTuplesInRawTB{0};
    size_t currentTupleEndrawTB{0};
    uint64_t numTotalBytesInrawTB{0};
    size_t numTuplesInTBFormatted{0};
    size_t currentFieldOffsetTBFormatted{0};

private:
    SequenceNumber sequenceNumber;
    ChunkNumber chunkNumber;
    OriginId originId;
    std::string tupleDelimiter;
    size_t tupleSizeInBytes{0};
    uint64_t numSchemaFields{0};
    std::string_view tupleBufferRawSV;
    std::string_view partialTuple;
    NES::Memory::TupleBuffer tupleBufferFormatted;

    size_t findIndexOfNextTuple() const { return tupleBufferRawSV.find(tupleDelimiter, currentTupleStartrawTB); }
};

template <typename T>
auto parseIntegerString()
{
    return [](const std::string& fieldValueString, int8_t* fieldPointer, NES::Memory::AbstractBufferProvider&, Memory::TupleBuffer&)
    {
        const auto sizeOfString = fieldValueString.size();
        T* value = reinterpret_cast<T*>(fieldPointer);
        auto [_, ec] = std::from_chars(fieldValueString.data(), fieldValueString.data() + sizeOfString, *value);
        if (ec == std::errc())
        {
            return;
        }
        if (ec == std::errc::invalid_argument)
        {
            throw CannotFormatMalformedStringValue(
                "Integer value '{}', is not a valid integer of type: {}.", fieldValueString, typeid(T).name());
        }
        if (ec == std::errc::result_out_of_range)
        {
            throw CannotFormatMalformedStringValue("Integer value '{}', is too large for type: {}.", fieldValueString, typeid(T).name());
        }
    };
}

void addBasicTypeParseFunction(
    const DataType::Type physicalType, std::vector<CSVInputFormatter::CastFunctionSignature>& fieldParseFunctions)
{
    switch (physicalType)
    {
        case NES::DataType::Type::INT8: {
            fieldParseFunctions.emplace_back(parseIntegerString<int8_t>());
            break;
        }
        case NES::DataType::Type::INT16: {
            fieldParseFunctions.emplace_back(parseIntegerString<int16_t>());
            break;
        }
        case NES::DataType::Type::INT32: {
            fieldParseFunctions.emplace_back(parseIntegerString<int32_t>());
            break;
        }
        case NES::DataType::Type::INT64: {
            fieldParseFunctions.emplace_back(parseIntegerString<int64_t>());
            break;
        }
        case NES::DataType::Type::UINT8: {
            fieldParseFunctions.emplace_back(parseIntegerString<uint8_t>());
            break;
        }
        case NES::DataType::Type::UINT16: {
            fieldParseFunctions.emplace_back(parseIntegerString<uint16_t>());
            break;
        }
        case NES::DataType::Type::UINT32: {
            fieldParseFunctions.emplace_back(parseIntegerString<uint32_t>());
            break;
        }
        case NES::DataType::Type::UINT64: {
            fieldParseFunctions.emplace_back(parseIntegerString<uint64_t>());
            break;
        }
        case NES::DataType::Type::FLOAT32: {
            const auto validateAndParseFloat
                = [](const std::string& fieldValueString, int8_t* fieldPointer, NES::Memory::AbstractBufferProvider&, Memory::TupleBuffer&)
            {
                if (fieldValueString.find(',') != std::string_view::npos)
                {
                    throw CannotFormatMalformedStringValue(
                        "Floats must use '.' as the decimal point. Parsed: {}, which uses ',' instead.", fieldValueString);
                }
                *reinterpret_cast<float*>(fieldPointer) = std::stof(fieldValueString);
            };
            fieldParseFunctions.emplace_back(std::move(validateAndParseFloat));
            break;
        }
        case NES::DataType::Type::FLOAT64: {
            const auto validateAndParseDouble
                = [](const std::string& fieldValueString, int8_t* fieldPointer, NES::Memory::AbstractBufferProvider&, Memory::TupleBuffer&)
            {
                if (fieldValueString.find(',') != std::string_view::npos)
                {
                    throw CannotFormatMalformedStringValue(
                        "Doubles must use '.' as the decimal point. Parsed: {}, which uses ',' instead.", fieldValueString);
                }
                *reinterpret_cast<double*>(fieldPointer) = std::stod(fieldValueString);
            };
            fieldParseFunctions.emplace_back(std::move(validateAndParseDouble));
            break;
        }
        case NES::DataType::Type::CHAR: {
            ///verify that only a single char was transmitted
            fieldParseFunctions.emplace_back(
                [](const std::string& inputString, int8_t* fieldPointer, Memory::AbstractBufferProvider&, Memory::TupleBuffer&)
                {
                    PRECONDITION(inputString.size() == 1, "A char must not have a size bigger than 1.");
                    const auto value = inputString.front();
                    *fieldPointer = value;
                    return sizeof(char);
                });
            break;
        }
        case NES::DataType::Type::BOOLEAN: {
            ///verify that a valid bool was transmitted (valid{true,false,0,1})
            fieldParseFunctions.emplace_back(
                [](const std::string& inputString, int8_t* fieldPointer, Memory::AbstractBufferProvider&, Memory::TupleBuffer&)
                {
                    const bool value = (strcasecmp(inputString.c_str(), "true") == 0) || (strcasecmp(inputString.c_str(), "1") == 0);
                    if (!value)
                    {
                        if ((static_cast<int>(strcasecmp(inputString.c_str(), "false") != 0) != 0)
                            && (strcasecmp(inputString.c_str(), "0") != 0))
                        {
                            NES_FATAL_ERROR(
                                "Parser::writeFieldValueToTupleBuffer: Received non boolean value for BOOLEAN field: {}",
                                inputString.c_str());
                            throw CannotFormatMalformedStringValue("Value: {} is not a boolean", inputString);
                        }
                    }
                    *fieldPointer = static_cast<int8_t>(value);
                    return sizeof(bool);
                });
            break;
        }
        case NES::DataType::Type::UNDEFINED: {
            NES_FATAL_ERROR("Parser::writeFieldValueToTupleBuffer: Field Type UNDEFINED");
            break;
        }
        default:
            NES_FATAL_ERROR("Unknown physical type: {}", magic_enum::enum_name(physicalType));
    }
}

CSVInputFormatter::CSVInputFormatter(const Schema& schema, std::string tupleDelimiter, std::string fieldDelimiter)
    : schema(schema), fieldDelimiter(std::move(fieldDelimiter)), tupleDelimiter(std::move(tupleDelimiter))
{
    this->fieldSizes.reserve(schema.getNumberOfFields());
    this->fieldParseFunctions.reserve(schema.getNumberOfFields());
    std::vector<DataType> physicalTypes;
    physicalTypes.reserve(schema.getNumberOfFields());
    for (const auto& field : schema.getFields())
    {
        physicalTypes.emplace_back(field.dataType);
    }

    /// Since we know the schema, we can create a vector that contains a function that converts the string representation of a field value
    /// to our internal representation in the correct order. During parsing, we iterate over the fields in each tuple, and, using the current
    /// field number, load the correct function for parsing from the vector.
    for (const auto& physicalType : physicalTypes)
    {
        /// Store the size of the field in bytes (for offset calculations).
        this->fieldSizes.emplace_back(physicalType.getSizeInBytes());
        /// Store the parsing function in a vector.
        if (physicalType.type != DataType::Type::VARSIZED)
        {
            addBasicTypeParseFunction(physicalType.type, this->fieldParseFunctions);
        }
        else
        {
            this->fieldParseFunctions.emplace_back(
                [](const std::string& inputString,
                   int8_t* fieldPointer,
                   Memory::AbstractBufferProvider& bufferProvider,
                   const NES::Memory::TupleBuffer& tupleBufferFormatted)
                {
                    NES_TRACE(
                        "Parser::writeFieldValueToTupleBuffer(): trying to write the variable length input string: {}"
                        "to tuple buffer",
                        inputString);
                    const auto valueLength = inputString.length();
                    auto childBuffer = bufferProvider.getUnpooledBuffer(valueLength + sizeof(uint32_t));
                    INVARIANT(childBuffer.has_value(), "Could not store string, because we cannot allocate a child buffer.");

                    auto& childBufferVal = childBuffer.value();
                    *childBufferVal.getBuffer<uint32_t>() = valueLength;
                    std::memcpy(childBufferVal.getBuffer<char>() + sizeof(uint32_t), inputString.data(), valueLength);
                    const auto index = tupleBufferFormatted.storeChildBuffer(childBufferVal);
                    auto* childBufferIndexPointer = reinterpret_cast<uint32_t*>(fieldPointer);
                    *childBufferIndexPointer = index;
                });
        }
    }
}

CSVInputFormatter::~CSVInputFormatter() = default;

void CSVInputFormatter::parseTupleBufferRaw(
    const NES::Memory::TupleBuffer& rawTB,
    PipelineExecutionContext& pipelineExecutionContext,
    const size_t numBytesInRawTB,
    SequenceShredder& sequenceShredder)
{
    PRECONDITION(rawTB.getBufferSize() != 0, "A raw tuple buffer must not be of empty.");

    const auto isInRange = sequenceShredder.isInRange(rawTB.getSequenceNumber().getRawValue());
    if (not(isInRange))
    {
        pipelineExecutionContext.emitBuffer(rawTB, NES::PipelineExecutionContext::ContinuationPolicy::REPEAT);
        return;
    }

    /// Creating a ProgressTracker on each call makes the CSVInputFormatter stateless
    auto progressTracker = ProgressTracker(
        rawTB.getSequenceNumber(), rawTB.getOriginId(), tupleDelimiter, schema.getSizeOfSchemaInBytes(), schema.getNumberOfFields());
    /// Reset all values that are tied to a specific rawTB.
    /// Also resets numTuplesInTBFormatted, because we always start with a new TBF when parsing a new TBR.
    progressTracker.resetForNewRawTB(numBytesInRawTB, rawTB.getBuffer<const char>(), 0, 0, 0);
    const auto sequenceNumberOfBuffer = rawTB.getSequenceNumber().getRawValue();
    constexpr auto maxSequenceNumber = std::numeric_limits<uint64_t>::max();
    const auto hasTupleDelimiter = progressTracker.getOffsetOfFirstTupleDelimiter() != maxSequenceNumber;
    const uint32_t offsetOfFirstTupleDelimiter = (hasTupleDelimiter) ? progressTracker.getOffsetOfFirstTupleDelimiter() : 0;
    const uint32_t offsetOfLastTupleDelimiter = (hasTupleDelimiter) ? progressTracker.getOffsetOfLastTupleDelimiter() : numBytesInRawTB;

    if (hasTupleDelimiter)
    {
        const auto [indexOfBuffer, buffersToFormat] = sequenceShredder.processSequenceNumber<true>(
            SequenceShredder::StagedBuffer{
                .buffer = rawTB,
                .sizeOfBufferInBytes = numBytesInRawTB,
                .offsetOfFirstTupleDelimiter = offsetOfFirstTupleDelimiter,
                .offsetOfLastTupleDelimiter = offsetOfLastTupleDelimiter},
            sequenceNumberOfBuffer);
        /// Skip, if there is only a single buffer, with a single tuple delimiter, since there are no possible tuples to parse
        if (buffersToFormat.size() == 1
            and (buffersToFormat[0].offsetOfFirstTupleDelimiter == buffersToFormat[0].offsetOfLastTupleDelimiter))
        {
            return;
        }
        /// 0. Allocate formatted buffer to write formatted tuples into.
        progressTracker.setNewTupleBufferFormatted(pipelineExecutionContext.allocateTupleBuffer());

        INVARIANT(indexOfBuffer < buffersToFormat.size(), "Index of buffer must be smaller than the size of the buffers.");

        /// 1. Process leading partial tuple if exists
        const auto hasLeadingPartialTuple = (indexOfBuffer != 0);
        if (hasLeadingPartialTuple)
        {
            processPartialTuple(0, indexOfBuffer, buffersToFormat, progressTracker, pipelineExecutionContext);
        }
        progressTracker.getTupleBufferFormatted().setNumberOfTuples(progressTracker.getNumTuplesInTBFormatted());

        /// 2. Process buffer itself
        const auto& bufferOfSequenceNumber = buffersToFormat.at(indexOfBuffer);
        progressTracker.resetForNewRawTB(
            bufferOfSequenceNumber.sizeOfBufferInBytes,
            bufferOfSequenceNumber.buffer.getBuffer<const char>(),
            bufferOfSequenceNumber.offsetOfFirstTupleDelimiter + tupleDelimiter.size(),
            bufferOfSequenceNumber.offsetOfLastTupleDelimiter);
        progressTracker.processCurrentBuffer(pipelineExecutionContext, fieldDelimiter, fieldParseFunctions, fieldSizes);

        /// 3. Process trailing  partial tuple if exists
        const auto hasTrailingPartialTuple = indexOfBuffer < buffersToFormat.size() - 1;
        if (hasTrailingPartialTuple)
        {
            /// We first need to check if the tuple can fit into the current formatted buffer
            progressTracker.checkAndHandleFullBuffer(pipelineExecutionContext);
            processPartialTuple(indexOfBuffer, buffersToFormat.size() - 1, buffersToFormat, progressTracker, pipelineExecutionContext);
        }
        auto finalFormattedBuffer = progressTracker.getTupleBufferFormatted();
        finalFormattedBuffer.setNumberOfTuples(progressTracker.getNumTuplesInTBFormatted());
        finalFormattedBuffer.setUsedMemorySize(finalFormattedBuffer.getNumberOfTuples() * schema.getSizeOfSchemaInBytes());
        finalFormattedBuffer.setLastChunk(true);

        pipelineExecutionContext.emitBuffer(finalFormattedBuffer, NES::PipelineExecutionContext::ContinuationPolicy::POSSIBLE);
    }
    else
    {
        const auto [_, buffersToFormat] = sequenceShredder.processSequenceNumber<false>(
            SequenceShredder::StagedBuffer{
                .buffer = rawTB,
                .sizeOfBufferInBytes = numBytesInRawTB,
                .offsetOfFirstTupleDelimiter = offsetOfFirstTupleDelimiter,
                .offsetOfLastTupleDelimiter = offsetOfLastTupleDelimiter},
            sequenceNumberOfBuffer);
        /// A buffer without a tuple delimiter can only find a spanning tuple, if it can reach a buffer with a spanning tuple to the left and right
        /// [buffer with tuple delimiter (TD)][buffer without TD][buffer with TD] <-- minimal spanning tuple for a buffer without TD
        if (buffersToFormat.size() < 3)
        {
            return;
        }

        /// Allocate formatted buffer to write formatted tuples into and process partial tuple.
        progressTracker.setNewTupleBufferFormatted(pipelineExecutionContext.allocateTupleBuffer());
        processPartialTuple(0, buffersToFormat.size() - 1, buffersToFormat, progressTracker, pipelineExecutionContext);
        auto finalFormattedBuffer = progressTracker.getTupleBufferFormatted();
        finalFormattedBuffer.setNumberOfTuples(finalFormattedBuffer.getNumberOfTuples() + 1);
        finalFormattedBuffer.setUsedMemorySize(finalFormattedBuffer.getNumberOfTuples() * schema.getSizeOfSchemaInBytes());
        finalFormattedBuffer.setLastChunk(true);
        pipelineExecutionContext.emitBuffer(finalFormattedBuffer, NES::PipelineExecutionContext::ContinuationPolicy::POSSIBLE);
    }
}

CSVInputFormatter::FormattedTupleIs CSVInputFormatter::processPartialTuple(
    const size_t partialTupleStartIdx,
    const size_t partialTupleEndIdx,
    const std::vector<SequenceShredder::StagedBuffer>& buffersToFormat,
    ProgressTracker& progressTracker,
    PipelineExecutionContext& pipelineExecutionContext)
{
    PRECONDITION(partialTupleStartIdx < partialTupleEndIdx, "Partial tuple start index must be smaller than the end index.");
    PRECONDITION(partialTupleEndIdx <= buffersToFormat.size(), "Partial tuple end index must be smaller than the size of the buffers.");

    /// If the buffers are not empty, there are at least three buffers
    /// 1. Process the first buffer
    const auto& firstBuffer = buffersToFormat[partialTupleStartIdx];
    const auto sizeOfLeadingPartialTuple
        = firstBuffer.sizeOfBufferInBytes - (firstBuffer.offsetOfLastTupleDelimiter + tupleDelimiter.size());
    INVARIANT(
        sizeOfLeadingPartialTuple <= firstBuffer.buffer.getBufferSize(),
        "Partial tuple is larger {} than buffer size {}",
        sizeOfLeadingPartialTuple,
        firstBuffer.buffer.getBufferSize());

    const std::string_view firstBytesOfPartialTuple = firstBuffer.buffer.getBuffer()
        ? std::string_view(
              firstBuffer.buffer.getBuffer<const char>() + (firstBuffer.offsetOfLastTupleDelimiter + 1), sizeOfLeadingPartialTuple)
        : std::string_view();

    std::string partialTuple(firstBytesOfPartialTuple);
    /// 2. Process all buffers in-between the first and the last
    for (size_t bufferIndex = partialTupleStartIdx + 1; bufferIndex < partialTupleEndIdx; ++bufferIndex)
    {
        const auto& currentBuffer = buffersToFormat[bufferIndex];
        const std::string_view intermediatePartialTuple
            = std::string_view(currentBuffer.buffer.getBuffer<const char>(), currentBuffer.sizeOfBufferInBytes);
        partialTuple.append(intermediatePartialTuple);
    }

    /// 3. Process the last buffer
    const auto& lastBuffer = buffersToFormat[partialTupleEndIdx];
    const std::string_view lastPartialTuple
        = std::string_view(lastBuffer.buffer.getBuffer<const char>(), lastBuffer.offsetOfFirstTupleDelimiter);
    partialTuple.append(lastPartialTuple);
    const auto stateOfPartialTuple = static_cast<FormattedTupleIs>(not(partialTuple.empty()));
    progressTracker.processCurrentTuple(
        std::move(partialTuple), fieldDelimiter, fieldParseFunctions, fieldSizes, *pipelineExecutionContext.getBufferManager());
    progressTracker.progressOneTuple();
    return stateOfPartialTuple;
}

void CSVInputFormatter::flushFinalTuple(
    OriginId originId, NES::PipelineExecutionContext& pipelineExecutionContext, SequenceShredder& sequenceShredder)
{
    const auto [resultBuffers, sequenceNumberToUseForFlushedTuple] = sequenceShredder.flushFinalPartialTuple();
    const auto flushedBuffers = std::move(resultBuffers.stagedBuffers);
    /// The sequence shredder insert a dummy buffer with a delimiter in its flush call.
    /// If it returns a single buffer, it is that dummy buffer and we can return.
    if (flushedBuffers.size() == 1)
    {
        return;
    }
    /// Get the final buffer (size - 1 is dummy buffer that just contains tuple delimiter)
    auto progressTracker = ProgressTracker(
        SequenceNumber(sequenceNumberToUseForFlushedTuple),
        originId,
        tupleDelimiter,
        schema.getSizeOfSchemaInBytes(),
        schema.getNumberOfFields());
    /// Allocate formatted buffer to write formatted tuples into.
    progressTracker.setNewTupleBufferFormatted(pipelineExecutionContext.allocateTupleBuffer());
    const auto formattedTupleIs
        = processPartialTuple(0, flushedBuffers.size() - 1, flushedBuffers, progressTracker, pipelineExecutionContext);
    /// Emit formatted buffer with single flushed tuple
    if (formattedTupleIs == FormattedTupleIs::NOT_EMPTY)
    {
        auto finalFormattedBuffer = progressTracker.getTupleBufferFormatted();
        finalFormattedBuffer.setNumberOfTuples(finalFormattedBuffer.getNumberOfTuples() + 1);
        finalFormattedBuffer.setUsedMemorySize(progressTracker.getNumTuplesInTBFormatted() * schema.getSizeOfSchemaInBytes());
        finalFormattedBuffer.setLastChunk(true);
        pipelineExecutionContext.emitBuffer(finalFormattedBuffer, NES::PipelineExecutionContext::ContinuationPolicy::POSSIBLE);
    }
}
size_t CSVInputFormatter::getSizeOfTupleDelimiter()
{
    return this->tupleDelimiter.size();
}
size_t CSVInputFormatter::getSizeOfFieldDelimiter()
{
    return this->fieldDelimiter.size();
}

std::ostream& CSVInputFormatter::toString(std::ostream& os) const
{
    os << "CSVInputFormatter: {\n";
    os << "  tuple delimiter: " << ((this->tupleDelimiter == "\n") ? "\\n" : this->tupleDelimiter) << ", \n";
    os << "  field delimiter: " << this->fieldDelimiter << ", \n";
    os << "  number of fields: " << this->fieldSizes.size() << ", \n";
    os << "  schema: " << schema;
    os << "\n}\n";

    return os;
}

InputFormatterRegistryReturnType
InputFormatterGeneratedRegistrar::RegisterCSVInputFormatter(InputFormatterRegistryArguments inputFormatterRegistryArguments)
{
    return std::make_unique<CSVInputFormatter>(
        inputFormatterRegistryArguments.schema,
        inputFormatterRegistryArguments.tupleDelimiter,
        inputFormatterRegistryArguments.fieldDelimiter);
}

}
