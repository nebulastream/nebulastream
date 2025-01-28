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
#include <format>
#include <functional>
#include <limits>
#include <memory>
#include <ostream>
#include <ranges>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>
#include <strings.h>
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <InputFormatters/InputFormatterOperatorHandler.hpp>
#include <InputFormatters/InputFormatterTask.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <boost/token_functions.hpp>
#include <boost/tokenizer.hpp>
#include <fmt/core.h>
#include <fmt/format.h>
#include <CSVInputFormatter.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatterRegistry.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include "InputFormatters/InputFormatter.hpp"
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
    ProgressTracker(std::string tupleDelimiter, const size_t tupleSizeInBytes, const size_t numberOfSchemaFields)
        : tupleDelimiter(std::move(tupleDelimiter)), tupleSizeInBytes(tupleSizeInBytes), numSchemaFields(numberOfSchemaFields) { };

    ~ProgressTracker() = default;

    void resetForNewRawTB(const size_t sizeOfNewTupleBufferRawInBytes, const char* newTupleBufferRaw, const size_t startOffset = 0, const size_t endOffset = 0)
    {
        this->numTotalBytesInrawTB = sizeOfNewTupleBufferRawInBytes;
        this->currentTupleStartrawTB = startOffset;
        this->endOfTuplesInRawTB = (endOffset == 0) ? sizeOfNewTupleBufferRawInBytes : endOffset + tupleSizeInBytes;
        this->currentFieldOffsetTBFormatted = 0;
        this->tupleBufferRawSV = std::string_view(newTupleBufferRaw, numTotalBytesInrawTB);
        this->currentTupleEndrawTB = findIndexOfNextTuple();
        this->numTuplesInTBFormatted = 0;
    }

    int8_t* getCurrentFieldPointer() { return (this->tupleBufferFormatted.getBuffer() + currentFieldOffsetTBFormatted); }

    [[nodiscard]] size_t getOffsetOfFirstTupleDelimiter() const { return currentTupleEndrawTB; }
    [[nodiscard]] size_t getOffsetOfLastTupleDelimiter() const { return tupleBufferRawSV.rfind(tupleDelimiter, tupleBufferRawSV.size() - 1); }

    [[nodiscard]] bool hasOneMoreTupleInRawTB() const { return currentTupleEndrawTB != std::numeric_limits<uint64_t>::max(); }

    [[nodiscard]] size_t sizeOfCurrentTuple() const { return this->currentTupleEndrawTB - this->currentTupleStartrawTB; }

    void progressOneTuple()
    {
        ++numTuplesInTBFormatted;
        currentTupleStartrawTB = currentTupleEndrawTB + 1;
        currentTupleEndrawTB = findIndexOfNextTuple();
    }

    [[nodiscard]] bool hasSpaceForTupleInTBFormatted() const
    {
        return tupleBufferFormatted.getBufferSize() - currentFieldOffsetTBFormatted >= tupleSizeInBytes;
    }

    [[nodiscard]] std::string_view getInitialPartialTuple(size_t sizeOfPartialTuple) const
    {
        return this->tupleBufferRawSV.substr(0, sizeOfPartialTuple);
    }

    [[nodiscard]] std::string_view getNextTuple() const
    {
        return this->tupleBufferRawSV.substr(currentTupleStartrawTB, sizeOfCurrentTuple());
    }

    void setNewTupleBufferFormatted(NES::Memory::TupleBuffer&& tupleBufferFormatted)
    {
        this->tupleBufferFormatted = std::move(tupleBufferFormatted);
    }

    std::string handlePriorPartialTuple()
    {
        std::string partialTuple;
        if(not(this->partialTuple.empty()))
        {
            partialTuple.resize(this->partialTuple.size() + currentTupleEndrawTB);
            partialTuple = std::string(this->partialTuple) + std::string(tupleBufferRawSV.substr(0, currentTupleEndrawTB));
        }
            return partialTuple;
    }

    std::string_view getEndingPartialTuple() const
    {
        return tupleBufferRawSV.substr(currentTupleStartrawTB, numTotalBytesInrawTB);
    }

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
    void setNumberOfTuplesInTBFormatted() { this->tupleBufferFormatted.setNumberOfTuples(numTuplesInTBFormatted); }
    uint64_t getNumTuplesInTBFormatted() { return numTuplesInTBFormatted; }
    const std::string& getTupleDelimiter() { return this->tupleDelimiter; }

    size_t currentTupleStartrawTB{0};
    size_t endOfTuplesInRawTB{0};
    size_t currentTupleEndrawTB{0};
    uint64_t numTotalBytesInrawTB{0};
    size_t numTuplesInTBFormatted{0};
    size_t currentFieldOffsetTBFormatted{0};

private:
    std::string tupleDelimiter;
    size_t tupleSizeInBytes{0};
    uint64_t numSchemaFields{0};
    std::string_view tupleBufferRawSV;
    std::string_view partialTuple{};
    NES::Memory::TupleBuffer tupleBufferFormatted;

    size_t findIndexOfNextTuple() const { return tupleBufferRawSV.find(tupleDelimiter, currentTupleStartrawTB); }
};

template <typename T>
auto parseIntegerString()
{
    return [](const std::string& fieldValueString, int8_t* fieldPointer, NES::Memory::AbstractBufferProvider&)
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
    const BasicPhysicalType& basicPhysicalType, std::vector<CSVInputFormatter::CastFunctionSignature>& fieldParseFunctions)
{
    switch (basicPhysicalType.nativeType)
    {
        case NES::BasicPhysicalType::NativeType::INT_8: {
            fieldParseFunctions.emplace_back(parseIntegerString<int8_t>());
            break;
        }
        case NES::BasicPhysicalType::NativeType::INT_16: {
            fieldParseFunctions.emplace_back(parseIntegerString<int16_t>());
            break;
        }
        case NES::BasicPhysicalType::NativeType::INT_32: {
            fieldParseFunctions.emplace_back(parseIntegerString<int32_t>());
            break;
        }
        case NES::BasicPhysicalType::NativeType::INT_64: {
            fieldParseFunctions.emplace_back(parseIntegerString<int64_t>());
            break;
        }
        case NES::BasicPhysicalType::NativeType::UINT_8: {
            fieldParseFunctions.emplace_back(parseIntegerString<uint8_t>());
            break;
        }
        case NES::BasicPhysicalType::NativeType::UINT_16: {
            fieldParseFunctions.emplace_back(parseIntegerString<uint16_t>());
            break;
        }
        case NES::BasicPhysicalType::NativeType::UINT_32: {
            fieldParseFunctions.emplace_back(parseIntegerString<uint32_t>());
            break;
        }
        case NES::BasicPhysicalType::NativeType::UINT_64: {
            fieldParseFunctions.emplace_back(parseIntegerString<uint64_t>());
            break;
        }
        case NES::BasicPhysicalType::NativeType::FLOAT: {
            const auto validateAndParseFloat
                = [](const std::string& fieldValueString, int8_t* fieldPointer, NES::Memory::AbstractBufferProvider&)
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
        case NES::BasicPhysicalType::NativeType::DOUBLE: {
            const auto validateAndParseDouble
                = [](const std::string& fieldValueString, int8_t* fieldPointer, NES::Memory::AbstractBufferProvider&)
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
        case NES::BasicPhysicalType::NativeType::CHAR: {
            ///verify that only a single char was transmitted
            fieldParseFunctions.emplace_back(
                [](const std::string& inputString, int8_t* fieldPointer, Memory::AbstractBufferProvider&)
                {
                    PRECONDITION(inputString.size() == 1, "A char must not have a size bigger than 1.");
                    const auto value = inputString.front();
                    *fieldPointer = value;
                    return sizeof(char);
                });
            break;
        }
        case NES::BasicPhysicalType::NativeType::BOOLEAN: {
            ///verify that a valid bool was transmitted (valid{true,false,0,1})
            fieldParseFunctions.emplace_back(
                [](const std::string& inputString, int8_t* fieldPointer, Memory::AbstractBufferProvider&)
                {
                    bool const value = (strcasecmp(inputString.c_str(), "true") == 0) || (strcasecmp(inputString.c_str(), "1") == 0);
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
        case NES::BasicPhysicalType::NativeType::UNDEFINED:
            NES_FATAL_ERROR("Parser::writeFieldValueToTupleBuffer: Field Type UNDEFINED");
    }
}

CSVInputFormatter::CSVInputFormatter(const Schema& schema, std::string tupleDelimiter, std::string fieldDelimiter)
    : fieldDelimiter(std::move(fieldDelimiter))
    , progressTracker(std::make_unique<ProgressTracker>(std::move(tupleDelimiter), schema.getSchemaSizeInBytes(), schema.getFieldCount()))
{
    this->fieldSizes.reserve(schema.getFieldCount());
    this->fieldParseFunctions.reserve(schema.getFieldCount());
    std::vector<std::shared_ptr<PhysicalType>> physicalTypes;
    const auto defaultPhysicalTypeFactory = DefaultPhysicalTypeFactory();
    physicalTypes.reserve(schema.getFieldCount());
    for (const AttributeFieldPtr& field : schema)
    {
        physicalTypes.emplace_back(defaultPhysicalTypeFactory.getPhysicalType(field->getDataType()));
    }

    /// Since we know the schema, we can create a vector that contains a function that converts the string representation of a field value
    /// to our internal representation in the correct order. During parsing, we iterate over the fields in each tuple, and, using the current
    /// field number, load the correct function for parsing from the vector.
    for (const auto& physicalType : physicalTypes)
    {
        /// Store the size of the field in bytes (for offset calculations).
        this->fieldSizes.emplace_back(physicalType->size());
        /// Store the parsing function in a vector.
        if (const auto basicPhysicalType = std::dynamic_pointer_cast<BasicPhysicalType>(physicalType))
        {
            addBasicTypeParseFunction(*basicPhysicalType, this->fieldParseFunctions);
        }
        else
        {
            this->fieldParseFunctions.emplace_back(
                [this](const std::string& inputString, int8_t* fieldPointer, Memory::AbstractBufferProvider& bufferProvider)
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
                    const auto index = progressTracker->getTupleBufferFormatted().storeChildBuffer(childBufferVal);
                    auto* childBufferIndexPointer = reinterpret_cast<uint32_t*>(fieldPointer);
                    *childBufferIndexPointer = index;
                    return sizeof(uint32_t);
                });
        }
    }
}

CSVInputFormatter::~CSVInputFormatter() = default;

void CSVInputFormatter::parseTupleBufferRaw(
    const NES::Memory::TupleBuffer& rawTB, //Todo: pass by value and move? Who owns the buffer?
    Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext,
    const size_t numBytesInRawTB, SequenceShredder& sequenceShredder)
{
    PRECONDITION(rawTB.getBufferSize() != 0, "A tuple buffer raw must not be of empty.");
    const auto isInRange = sequenceShredder.isSequenceNumberInRange(rawTB.getSequenceNumber().getRawValue());
    if(not(isInRange))
    {
        pipelineExecutionContext.emitBuffer(
                    rawTB,
                    NES::Runtime::Execution::PipelineExecutionContext::ContinuationPolicy::NEVER); /// Todo: emit buffer with RE_EMIT policy
        return;
    }
    /// Reset all values that are tied to a specific rawTB.
    /// Also resets numTuplesInTBFormatted, because we always start with a new TBF when parsing a new TBR.
    progressTracker->resetForNewRawTB(numBytesInRawTB, rawTB.getBuffer<const char>());

    const auto isFirstSequenceNumber = rawTB.getSequenceNumber().getRawValue() == SequenceNumber::INITIAL;
    const auto maxSequenceNumber = std::numeric_limits<uint64_t>::max();
    /// The first buffer has an 'implicit' tuple delimiter
    // If there is no tuple delimiter, the return value is max
    auto offsetOfFirstTupleDelimiter = (isFirstSequenceNumber) ? 0 : progressTracker->getOffsetOfFirstTupleDelimiter();
    offsetOfFirstTupleDelimiter += static_cast<uint64_t>(offsetOfFirstTupleDelimiter == maxSequenceNumber);
    auto offsetOfLastTupleDelimiter = progressTracker->getOffsetOfLastTupleDelimiter();
    offsetOfLastTupleDelimiter += static_cast<uint64_t>(offsetOfLastTupleDelimiter == maxSequenceNumber);
    const auto hasTupleDelimiter = offsetOfFirstTupleDelimiter != maxSequenceNumber;
    const auto numUses = static_cast<uint8_t>(2 - (not(hasTupleDelimiter) or isFirstSequenceNumber));
    // Todo: add third buffer without delimiter in between two buffers with delimiter
    const auto buffersToFormat = (hasTupleDelimiter)
        ? sequenceShredder.processSequenceNumber<true>(SequenceShredder::StagedBuffer{.buffer=rawTB, .sizeOfBufferInBytes=numBytesInRawTB,
            .offsetOfFirstTupleDelimiter=offsetOfFirstTupleDelimiter, .offsetOfLastTupleDelimiter=offsetOfLastTupleDelimiter, .uses=numUses})
        : sequenceShredder.processSequenceNumber<false>(SequenceShredder::StagedBuffer{.buffer=rawTB, .sizeOfBufferInBytes=numBytesInRawTB,
            .offsetOfFirstTupleDelimiter=offsetOfFirstTupleDelimiter, .offsetOfLastTupleDelimiter=offsetOfLastTupleDelimiter, .uses=numUses});

    if (buffersToFormat.empty())
    {
        // Todo: is it ok to simply return?
        return;
    }
    /// At least one tuple ends in the current TBR, allocate a new output tuple buffer for the parsed data.
    progressTracker->setNewTupleBufferFormatted(pipelineExecutionContext.allocateTupleBuffer());
    std::string partialTuple;
    for (size_t bufferIdx = 0; const auto& stagedBuffer : buffersToFormat)
    {
        const auto startOffset = (buffersToFormat.size() > 1 and bufferIdx == buffersToFormat.size() - 1) ? 0 : stagedBuffer.offsetOfFirstTupleDelimiter;
        const auto endOffset = stagedBuffer.offsetOfLastTupleDelimiter;
        progressTracker->resetForNewRawTB(stagedBuffer.sizeOfBufferInBytes, stagedBuffer.buffer.getBuffer<const char>(), startOffset, endOffset);
        /// Process partial tuple of previous buffer, if given, then start to process the rest of the buffer, if there is more
        if (not(partialTuple.empty()))
        {
            partialTuple.reserve(partialTuple.size() + stagedBuffer.offsetOfFirstTupleDelimiter);
            partialTuple += std::string(progressTracker->getInitialPartialTuple(stagedBuffer.offsetOfFirstTupleDelimiter));
            parseStringIntoTupleBuffer(partialTuple, *pipelineExecutionContext.getBufferManager());
            progressTracker->progressOneTuple();
        }

        /// While the start of the next tuple is not the endOffset (+ sizeOfTupleInBytes) get the next tuple
        while (progressTracker->hasOneMoreTupleInRawTB())
        {
            /// If we parsed a (prior) partial tuple before, the TBF may not fit another tuple. We emit the single (prior) partial tuple.
            /// Otherwise, the TBF is empty. Since we assume that a tuple is never bigger than a TBF, we can fit at least one more tuple.
            if (not progressTracker->hasSpaceForTupleInTBFormatted())
            {
                /// Emit TBF and get new TBF
                progressTracker->setNumberOfTuplesInTBFormatted();
                NES_TRACE("emitting TupleBuffer with {} tuples.", progressTracker->numTuplesInTBFormatted);
                /// true triggers adding sequence number, etc.
                pipelineExecutionContext.emitBuffer(
                    progressTracker->getTupleBufferFormatted(),
                    NES::Runtime::Execution::PipelineExecutionContext::ContinuationPolicy::POSSIBLE);
                progressTracker->setNewTupleBufferFormatted(pipelineExecutionContext.allocateTupleBuffer());
                progressTracker->currentFieldOffsetTBFormatted = 0;
                progressTracker->numTuplesInTBFormatted = 0;
            }

            const auto currentTuple = progressTracker->getNextTuple();
            parseStringIntoTupleBuffer(currentTuple, *pipelineExecutionContext.getBufferManager());

            progressTracker->progressOneTuple();
        }
        ++bufferIdx;
        //Todo: improve: handle via indexes?
        /// only set partial tuple, if there is another iteration that may use it
        if (bufferIdx != buffersToFormat.size())
        {
            partialTuple = progressTracker->getEndingPartialTuple();
        }
    }

    progressTracker->setNumberOfTuplesInTBFormatted();
    NES_TRACE("emitting parsed tuple buffer with {} tuples.", progressTracker->numTuplesInTBFormatted);

    if (progressTracker->getNumTuplesInTBFormatted() != 0)
    {
        /// Emit the current TBF, even if there is only a single tuple (there is at least one) in it.
        pipelineExecutionContext.emitBuffer(
            progressTracker->getTupleBufferFormatted(), NES::Runtime::Execution::PipelineExecutionContext::ContinuationPolicy::POSSIBLE);
    }
}

void CSVInputFormatter::parseStringIntoTupleBuffer(
    const std::string_view stringTuple, NES::Memory::AbstractBufferProvider& bufferProvider) const
{
    const boost::char_separator sep{this->fieldDelimiter.c_str()};
    const boost::tokenizer tupleTokenizer{stringTuple.begin(), stringTuple.end(), sep};

    /// Iterate over all fields, parse the string values and write the formatted data into the TBF.
    for (auto [token, parseFunction, fieldSize] : std::views::zip(tupleTokenizer, fieldParseFunctions, fieldSizes))
    {
        parseFunction(token, progressTracker->getCurrentFieldPointer(), bufferProvider);
        const auto lastVal = *reinterpret_cast<int32_t*>(progressTracker->getCurrentFieldPointer());
        std::cout << lastVal << std::endl;
        progressTracker->currentFieldOffsetTBFormatted += fieldSize;
    }
}

std::ostream& CSVInputFormatter::toString(std::ostream& str) const
{
    str << std::format("CSVInputFormatter(fieldDelimiter: '{}'", this->fieldDelimiter);
    str << ", progressTracker: " << *this->progressTracker;
    return str;
}

std::unique_ptr<NES::InputFormatters::InputFormatter>
InputFormatterGeneratedRegistrar::RegisterCSVInputFormatter(const Schema& schema, std::string tupleDelimiter, std::string fieldDelimiter)
{
    std::unique_ptr<NES::InputFormatters::InputFormatter> inputFormatter = std::make_unique<CSVInputFormatter>(schema, std::move(tupleDelimiter), std::move(fieldDelimiter));
    return inputFormatter;
}

}
