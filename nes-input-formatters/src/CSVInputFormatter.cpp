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
#include <InputFormatters/InputFormatter.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/PinnedBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
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

namespace NES::InputFormatters
{

struct PartialTuple
{
    uint64_t offsetInBuffer;
    Memory::PinnedBuffer tbRaw;

    [[nodiscard]] std::string_view getStringView() const
    {
        return {tbRaw.getBuffer<const char>() + offsetInBuffer, tbRaw.getBufferSize() - offsetInBuffer};
    }
};

class ProgressTracker
{
public:
    ProgressTracker(std::string tupleDelimiter, const size_t tupleSizeInBytes, const size_t numberOfSchemaFields)
        : tupleDelimiter(std::move(tupleDelimiter)), tupleSizeInBytes(tupleSizeInBytes), numSchemaFields(numberOfSchemaFields) {};

    ~ProgressTracker() = default;

    void resetForNewTBRaw(const size_t sizeOfNewTupleBufferRawInBytes, const char* newTupleBufferRaw)
    {
        this->numTotalBytesInTBRaw = sizeOfNewTupleBufferRawInBytes;
        this->currentTupleStartTBRaw = 0;
        this->currentFieldOffsetTBFormatted = 0;
        this->tupleBufferRawSV = std::string_view(newTupleBufferRaw, numTotalBytesInTBRaw);
        this->currentTupleEndTBRaw = findIndexOfNextTuple();
        this->numTuplesInTBFormatted = 0;
    }

    int8_t* getCurrentFieldPointer() { return (this->tupleBufferFormatted.getBuffer() + currentFieldOffsetTBFormatted); }

    [[nodiscard]] bool hasOneMoreTupleInTBRaw() const { return this->currentTupleEndTBRaw != std::string::npos; }

    [[nodiscard]] size_t sizeOfCurrentTuple() const { return this->currentTupleEndTBRaw - this->currentTupleStartTBRaw; }

    void progressOneTuple()
    {
        ++numTuplesInTBFormatted;
        currentTupleStartTBRaw = currentTupleEndTBRaw + 1;
        currentTupleEndTBRaw = findIndexOfNextTuple();
    }

    [[nodiscard]] bool hasSpaceForTupleInTBFormatted() const
    {
        return tupleBufferFormatted.getBufferSize() - currentFieldOffsetTBFormatted >= tupleSizeInBytes;
    }

    [[nodiscard]] bool hasPartialTuple() const { return not stagingAreaTBRaw.empty(); }

    [[nodiscard]] std::string_view getNextTuple() const
    {
        return this->tupleBufferRawSV.substr(currentTupleStartTBRaw, sizeOfCurrentTuple());
    }

    void setNewTupleBufferFormatted(NES::Memory::PinnedBuffer&& tupleBufferFormatted)
    {
        this->tupleBufferFormatted = std::move(tupleBufferFormatted);
    }

    void handleResidualBytes(NES::Memory::PinnedBuffer tbRawEndingInPartialTuple)
    {
        if (currentTupleStartTBRaw < numTotalBytesInTBRaw)
        {
            /// write TBR to staging area
            stagingAreaTBRaw.emplace_back(
                PartialTuple{.offsetInBuffer = currentTupleStartTBRaw, .tbRaw = std::move(tbRawEndingInPartialTuple)});
        }
    }

    friend std::ostream& operator<<(std::ostream& out, const ProgressTracker& progressTracker)
    {
        return out << fmt::format(
                   "tupleDelimiter: {}, size of tuples in bytes: {}, number of fields in schema: {}, tbRaw(total number of bytes: {}, "
                   "start of "
                   "current tuple: {}, end of current tuple: {}), tbFormatted(number of tuples: {}, current field offset: {})",
                   (progressTracker.tupleDelimiter == "\n") ? "\\n" : progressTracker.tupleDelimiter,
                   progressTracker.tupleSizeInBytes,
                   progressTracker.numSchemaFields,
                   progressTracker.numTotalBytesInTBRaw,
                   progressTracker.currentTupleStartTBRaw,
                   progressTracker.currentTupleEndTBRaw,
                   progressTracker.numTuplesInTBFormatted,
                   progressTracker.currentFieldOffsetTBFormatted);
    }

    /// Getter & Setter
    [[nodiscard]] uint64_t getNumSchemaFields() const { return this->numSchemaFields; }
    NES::Memory::PinnedBuffer& getTupleBufferFormatted() { return this->tupleBufferFormatted; }
    void setNumberOfTuplesInTBFormatted() { this->tupleBufferFormatted.setNumberOfTuples(numTuplesInTBFormatted); }
    const std::string& getTupleDelimiter() { return this->tupleDelimiter; }

    size_t currentTupleStartTBRaw{0};
    size_t currentTupleEndTBRaw{0};
    uint64_t numTotalBytesInTBRaw{0};
    std::vector<PartialTuple> stagingAreaTBRaw;
    size_t numTuplesInTBFormatted{0};
    size_t currentFieldOffsetTBFormatted{0};

private:
    std::string tupleDelimiter;
    size_t tupleSizeInBytes{0};
    uint64_t numSchemaFields{0};
    std::string_view tupleBufferRawSV;
    NES::Memory::PinnedBuffer tupleBufferFormatted;

    size_t findIndexOfNextTuple() { return tupleBufferRawSV.find(tupleDelimiter, currentTupleStartTBRaw); }
};

template <typename T>
auto parseIntegerString()
{
    return [](const std::string& fieldValueString, int8_t* fieldPointer, NES::Memory::AbstractBufferProvider&)
    {
        if (auto integer = Util::from_chars<T>(fieldValueString))
        {
            *reinterpret_cast<T*>(fieldPointer) = *integer;
        }
        else
        {
            throw CannotFormatMalformedStringValue(
                "Integer value '{}', is not a valid integer of type: {}.", fieldValueString, typeid(T).name());
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
    for (const std::shared_ptr<AttributeField>& field : schema)
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
                    const auto index = progressTracker->getTupleBufferFormatted().storeReturnChildIndex(std::move(*childBuffer));
                    INVARIANT(index, "Failed to store child buffer in CSV input formatter");
                    auto* childBufferIndexPointer = reinterpret_cast<uint32_t*>(fieldPointer);
                    *childBufferIndexPointer = index->getNum();
                    return sizeof(uint32_t);
                });
        }
    }
}

CSVInputFormatter::~CSVInputFormatter() = default;

void CSVInputFormatter::parseTupleBufferRaw(
    const NES::Memory::PinnedBuffer& tbRaw,
    NES::Memory::AbstractBufferProvider& bufferProvider,
    const size_t numBytesInTBRaw,
    const std::function<void(Memory::PinnedBuffer& buffer, bool addBufferMetaData)>& emitFunction)
{
    PRECONDITION(tbRaw.getBufferSize() != 0, "A tuple buffer raw must not be of empty.");
    /// Reset all values that are tied to a specific tbRaw.
    /// Also resets numTuplesInTBFormatted, because we always start with a new TBF when parsing a new TBR.
    progressTracker->resetForNewTBRaw(numBytesInTBRaw, tbRaw.getBuffer<const char>());

    /// Determine if the current TBR terminates at least one tuple.
    if (not progressTracker->hasOneMoreTupleInTBRaw())
    {
        /// If there is not a single complete tuple in the buffer, write it to the staging area and wait for the next buffer(s).
        progressTracker->stagingAreaTBRaw.emplace_back(PartialTuple{.offsetInBuffer = 0, .tbRaw = tbRaw});
        return;
    }
    /// At least one tuple ends in the current TBR, allocate a new output tuple buffer for the parsed data.
    progressTracker->setNewTupleBufferFormatted(bufferProvider.getBufferBlocking());

    /// A single partial tuple may have spanned over the prior N TBRs, ending in the current TBR. If so, construct the tuple using the prior TBRs.
    /// The size of the partial tuple may reach from just the last byte of the prior TBR to all bytes of multiple prior TBRs.
    if (progressTracker->hasPartialTuple())
    {
        /// Construct the full tuple from all prior tuple buffers that contain a part of the partial tuple.
        std::stringstream completeTuple;
        for (const auto& partialTuple : progressTracker->stagingAreaTBRaw)
        {
            completeTuple << partialTuple.getStringView();
        }
        /// Add the final part of the partial tuple from the current TBR to the partial tuple to complete it.
        completeTuple << progressTracker->getNextTuple();
        parseStringIntoTupleBuffer(completeTuple.str(), bufferProvider);
        /// Release all prior TBRs with partial tuples. Sources can now use these buffers again.
        progressTracker->stagingAreaTBRaw.clear();
        /// We parsed the first tuple terminated by the current TBR. If the current TBR does not terminate another tuple, we skip the below while loop.
        progressTracker->progressOneTuple();
    }

    /// Parse the current TBR, while there is at least one more tuple in the tuple buffer (we use '\n' as a hardcoded separator for now).
    /// We always parse at least one tuple if we enter the while loop.
    while (progressTracker->hasOneMoreTupleInTBRaw())
    {
        /// If we parsed a (prior) partial tuple before, the TBF may not fit another tuple. We emit the single (prior) partial tuple.
        /// Otherwise, the TBF is empty. Since we assume that a tuple is never bigger than a TBF, we can fit at least one more tuple.
        if (not progressTracker->hasSpaceForTupleInTBFormatted())
        {
            /// Emit TBF and get new TBF
            progressTracker->setNumberOfTuplesInTBFormatted();
            NES_TRACE("emitting TupleBuffer with {} tuples.", progressTracker->numTuplesInTBFormatted);
            emitFunction(progressTracker->getTupleBufferFormatted(), true); /// true triggers adding sequence number, etc.
            progressTracker->setNewTupleBufferFormatted(bufferProvider.getBufferBlocking());
            progressTracker->currentFieldOffsetTBFormatted = 0;
            progressTracker->numTuplesInTBFormatted = 0;
        }

        const auto currentTuple = progressTracker->getNextTuple();
        parseStringIntoTupleBuffer(currentTuple, bufferProvider);

        progressTracker->progressOneTuple();
    }

    progressTracker->setNumberOfTuplesInTBFormatted();
    NES_TRACE("emitting parsed tuple buffer with {} tuples.", progressTracker->numTuplesInTBFormatted);

    /// Emit the current TBF, even if there is only a single tuple (there is at least one) in it.
    emitFunction(progressTracker->getTupleBufferFormatted(), /* add metadata */ true);
    progressTracker->handleResidualBytes(tbRaw);
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
        progressTracker->currentFieldOffsetTBFormatted += fieldSize;
    }
}

std::ostream& CSVInputFormatter::toString(std::ostream& str) const
{
    str << std::format("CSVInputFormatter(fieldDelimiter: '{}'", this->fieldDelimiter);
    str << ", progressTracker: " << *this->progressTracker;
    return str;
}

std::unique_ptr<InputFormatterRegistryReturnType>
InputFormatterGeneratedRegistrar::RegisterCSVInputFormatter(InputFormatterRegistryArguments inputFormatterRegistryArguments)
{
    return std::make_unique<CSVInputFormatter>(
        inputFormatterRegistryArguments.schema,
        inputFormatterRegistryArguments.tupleDelimiter,
        inputFormatterRegistryArguments.fieldDelimiter);
}

}
