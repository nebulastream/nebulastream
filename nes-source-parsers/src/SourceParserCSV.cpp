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
#include <functional>
#include <memory>
#include <ranges>
#include <sstream>
#include <string>
#include <string_view>
#include <API/AttributeField.hpp>
#include <Exceptions/RuntimeException.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SourceParsers/SourceParser.hpp>
#include <SourceParsers/SourceParserCSV.hpp>
#include <SourceParsers/SourceParserRegistry.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <boost/token_functions.hpp>
#include <boost/tokenizer.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>

namespace NES::SourceParsers
{

struct PartialTuple
{
    uint64_t offsetInBuffer;
    Memory::TupleBuffer tbRaw;

    std::string_view getStringView() const
    {
        return std::string_view(tbRaw.getBuffer<const char>() + offsetInBuffer, tbRaw.getBufferSize() - offsetInBuffer);
    }
};

class ProgressTracker
{
public:
    ProgressTracker(std::string tupleSeparator, const size_t tupleSizeInBytes, const size_t numberOfSchemaFields)
        : tupleSeparator(std::move(tupleSeparator)), tupleSizeInBytes(tupleSizeInBytes), numSchemaFields(numberOfSchemaFields){};

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

    int8_t* getCurrentFieldPointer() const { return rawBufferFieldPointer + currentFieldOffsetTBFormatted; }

    bool hasOneMoreTupleInTBRaw() const { return this->currentTupleEndTBRaw != std::string::npos; }

    size_t sizeOfCurrentTuple() const { return this->currentTupleEndTBRaw - this->currentTupleStartTBRaw; }

    void progressOneTuple()
    {
        ++numTuplesInTBFormatted;
        currentTupleStartTBRaw = currentTupleEndTBRaw + 1;
        currentTupleEndTBRaw = findIndexOfNextTuple();
    }

    bool hasSpaceForTupleInTBFormatted() const
    {
        return tupleBufferFormatted.getBufferSize() - currentFieldOffsetTBFormatted >= tupleSizeInBytes;
    }

    bool hasPartialTuple() const { return not stagingAreaTBRaw.empty(); }

    std::string_view getNextTuple() const { return this->tupleBufferRawSV.substr(currentTupleStartTBRaw, sizeOfCurrentTuple()); }

    void setNewTupleBufferFormatted(NES::Memory::TupleBuffer&& tupleBufferFormatted)
    {
        this->tupleBufferFormatted = std::move(tupleBufferFormatted);
        this->rawBufferFieldPointer = this->tupleBufferFormatted.getBuffer();
    }

    void handleResidualBytes(NES::Memory::TupleBuffer tbRawEndingInPartialTuple)
    {
        if (currentTupleStartTBRaw < numTotalBytesInTBRaw)
        {
            /// write TBR to staging area
            stagingAreaTBRaw.emplace_back(
                PartialTuple{.offsetInBuffer = currentTupleStartTBRaw, .tbRaw = std::move(tbRawEndingInPartialTuple)});
        }
    }

    /// Getter & Setter
    uint64_t getNumSchemaFields() const { return this->numSchemaFields; }
    NES::Memory::TupleBuffer& getTupleBufferFormatted() { return this->tupleBufferFormatted; }
    void setNumberOfTuplesInTBFormatted() { this->tupleBufferFormatted.setNumberOfTuples(numTuplesInTBFormatted); }

    size_t currentTupleStartTBRaw{0};
    size_t currentTupleEndTBRaw{0};
    uint64_t numTotalBytesInTBRaw{0};
    std::vector<PartialTuple> stagingAreaTBRaw;
    size_t numTuplesInTBFormatted{0};
    size_t currentFieldOffsetTBFormatted{0};

private:
    std::string tupleSeparator;
    size_t tupleSizeInBytes{0};
    uint64_t numSchemaFields{0};
    std::string_view tupleBufferRawSV;
    int8_t* rawBufferFieldPointer{nullptr}; /// owned by TBRaw, reset during each call of 'parseTupleBufferRaw()'
    NES::Memory::TupleBuffer tupleBufferFormatted;

    size_t findIndexOfNextTuple() { return tupleBufferRawSV.find(tupleSeparator, currentTupleStartTBRaw); }
};

template <typename T>
auto parseNumericString(std::optional<std::function<bool(const std::string&)>> typeValidateFunc = std::nullopt)
{
    return [validateFunc
            = std::move(typeValidateFunc)](const std::string& fieldValueString, int8_t* fieldPointer, NES::Memory::AbstractBufferProvider&)
    {
        if (validateFunc.has_value() && not(validateFunc.value()(fieldValueString)))
        {
            throw DifferentFieldTypeExpected("Could not parse field value: {}", fieldValueString);
        }
        T* value = reinterpret_cast<T*>(fieldPointer);
        std::from_chars(fieldValueString.data(), fieldValueString.data() + fieldValueString.size(), *value);
    };
}

SourceParserCSV::SourceParserCSV(SchemaPtr schema, std::string tupleSeparator, std::string delimiter)
    : schema(std::move(schema))
    , fieldDelimiter(std::move(delimiter))
    , progressTracker(std::make_unique<ProgressTracker>(std::move(tupleSeparator), this->schema->getSchemaSizeInBytes(), this->schema->getSize()))
{
    std::vector<std::shared_ptr<PhysicalType>> physicalTypes;
    const auto defaultPhysicalTypeFactory = DefaultPhysicalTypeFactory();
    for (const AttributeFieldPtr& field : this->schema->fields)
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
            switch (basicPhysicalType->nativeType)
            {
                case NES::BasicPhysicalType::NativeType::INT_8: {
                    this->fieldParseFunctions.emplace_back(parseNumericString<int8_t>());
                    break;
                }
                case NES::BasicPhysicalType::NativeType::INT_16: {
                    this->fieldParseFunctions.emplace_back(parseNumericString<int16_t>());
                    break;
                }
                case NES::BasicPhysicalType::NativeType::INT_32: {
                    this->fieldParseFunctions.emplace_back(parseNumericString<int32_t>());
                    break;
                }
                case NES::BasicPhysicalType::NativeType::INT_64: {
                    this->fieldParseFunctions.emplace_back(parseNumericString<int64_t>());
                    break;
                }
                case NES::BasicPhysicalType::NativeType::UINT_8: {
                    this->fieldParseFunctions.emplace_back(parseNumericString<uint8_t>());
                    break;
                }
                case NES::BasicPhysicalType::NativeType::UINT_16: {
                    this->fieldParseFunctions.emplace_back(parseNumericString<uint16_t>());
                    break;
                }
                case NES::BasicPhysicalType::NativeType::UINT_32: {
                    this->fieldParseFunctions.emplace_back(parseNumericString<uint32_t>());
                    break;
                }
                case NES::BasicPhysicalType::NativeType::UINT_64: {
                    this->fieldParseFunctions.emplace_back(parseNumericString<uint64_t>());
                    break;
                }
                case NES::BasicPhysicalType::NativeType::FLOAT: {
                    const auto validateFloat = [](const std::string_view fieldValueString)
                    {
                        if (fieldValueString.find(',') != std::string_view::npos)
                        {
                            NES_ERROR("Floats must not contain ',' as the decimal point.");
                            return false;
                        }
                        return true;
                    };
                    this->fieldParseFunctions.emplace_back(parseNumericString<float>(std::move(validateFloat)));
                    break;
                }
                case NES::BasicPhysicalType::NativeType::DOUBLE: {
                    const auto validateFloat = [](const std::string_view fieldValueString)
                    {
                        if (fieldValueString.find(',') != std::string_view::npos)
                        {
                            NES_ERROR("Floats must not contain ',' as the decimal point.");
                            return false;
                        }
                        return true;
                    };
                    this->fieldParseFunctions.emplace_back(parseNumericString<double>(std::move(validateFloat)));
                    break;
                }
                case NES::BasicPhysicalType::NativeType::CHAR: {
                    ///verify that only a single char was transmitted
                    this->fieldParseFunctions.emplace_back(
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
                    this->fieldParseFunctions.emplace_back(
                        [](const std::string& inputString, int8_t* fieldPointer, Memory::AbstractBufferProvider&)
                        {
                            bool value = !strcasecmp(inputString.c_str(), "true") || !strcasecmp(inputString.c_str(), "1");
                            if (!value)
                            {
                                if (strcasecmp(inputString.c_str(), "false") && strcasecmp(inputString.c_str(), "0"))
                                {
                                    NES_FATAL_ERROR(
                                        "Parser::writeFieldValueToTupleBuffer: Received non boolean value for BOOLEAN field: {}",
                                        inputString.c_str());
                                    throw std::invalid_argument("Value " + inputString + " is not a boolean");
                                }
                            }
                            *fieldPointer = value;
                            return sizeof(bool);
                        });
                    break;
                }
                case NES::BasicPhysicalType::NativeType::UNDEFINED:
                    NES_FATAL_ERROR("Parser::writeFieldValueToTupleBuffer: Field Type UNDEFINED");
            }
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
                    INVARIANT(childBuffer.has_value(), "Could not store string, because allocated child buffer did not contain value");

                    auto& childBufferVal = childBuffer.value();
                    *childBufferVal.getBuffer<uint32_t>() = valueLength;
                    std::memcpy(childBufferVal.getBuffer<char>() + sizeof(uint32_t), inputString.data(), valueLength);
                    const auto index = progressTracker->getTupleBufferFormatted().storeChildBuffer(childBufferVal);
                    auto childBufferIndexPointer = reinterpret_cast<uint32_t*>(fieldPointer);
                    *childBufferIndexPointer = index;
                    return sizeof(uint32_t);
                });
        }
    }
}

SourceParserCSV::~SourceParserCSV() = default;

bool SourceParserCSV::parseTupleBufferRaw(
    const NES::Memory::TupleBuffer& tbRaw,
    NES::Memory::AbstractBufferProvider& bufferManager,
    const size_t numBytesInTBRaw,
    const std::function<void(Memory::TupleBuffer& buffer, bool addBufferMetaData)>& emitFunction)
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
        return true;
    }
    /// At least one tuple ends in the current TBR, allocate a new output tuple buffer for the parsed data.
    progressTracker->setNewTupleBufferFormatted(bufferManager.getBufferBlocking());

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
        parseStringTupleToTBFormatted(completeTuple.str(), bufferManager);
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
            NES_DEBUG("emitting TupleBuffer with {} tuples.", progressTracker->numTuplesInTBFormatted);
            emitFunction(progressTracker->getTupleBufferFormatted(), true); /// true triggers adding sequence number, etc.
            progressTracker->setNewTupleBufferFormatted(bufferManager.getBufferBlocking());
            progressTracker->currentFieldOffsetTBFormatted = 0;
            progressTracker->numTuplesInTBFormatted = 0;
        }

        const auto currentTuple = progressTracker->getNextTuple();
        parseStringTupleToTBFormatted(currentTuple, bufferManager);

        progressTracker->progressOneTuple();
    }

    progressTracker->setNumberOfTuplesInTBFormatted();
    NES_DEBUG("emitting parsed tuple buffer with {} tuples.", progressTracker->numTuplesInTBFormatted);

    /// Emit the current TBF, even if there is only a single tuple (there is at least one) in it.
    emitFunction(progressTracker->getTupleBufferFormatted(), /* add metadata */ true);
    progressTracker->handleResidualBytes(tbRaw);

    return true;
}

void SourceParserCSV::parseStringTupleToTBFormatted(
    const std::string_view stringTuple, NES::Memory::AbstractBufferProvider& bufferProvider) const
{
    const boost::char_separator sep{this->fieldDelimiter.c_str()};
    const boost::tokenizer tupleTokenizer{stringTuple, sep};

    /// Iterate over all (potentially except the first) fields, parse the string values and write the formatted data into the TBF.
    for (size_t currentFieldNumber = 0; const auto& currentVal : tupleTokenizer)
    {
        fieldParseFunctions.at(currentFieldNumber)(currentVal, progressTracker->getCurrentFieldPointer(), bufferProvider);
        progressTracker->currentFieldOffsetTBFormatted += this->fieldSizes.at(currentFieldNumber);
        ++currentFieldNumber;
    }
}

std::unique_ptr<SourceParser>
SourceParserGeneratedRegistrar::RegisterSourceParserCSV(std::shared_ptr<Schema> schema, std::string tupleSeparator, std::string fieldDelimiter)
{
    return std::make_unique<SourceParserCSV>(std::move(schema), std::move(tupleSeparator), std::move(fieldDelimiter));
}

}
