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

#include <memory>
#include <ranges>
#include <sstream>
#include <string>
#include <string_view>
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Exceptions/RuntimeException.hpp>
#include <SourceParsers/SourceParser.hpp>
#include <SourceParsers/SourceParserCSV.hpp>
#include <SourceParsers/SourceParserRegistry.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>
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
        : tupleSeparator(std::move(tupleSeparator)), tupleSizeInBytes(tupleSizeInBytes), numSchemaFields(numberOfSchemaFields) {};

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

    friend std::ostream& operator<<(std::ostream& out, const ProgressTracker& progressTracker)
    {
        return out << fmt::format(
                   "tupleSeparator: {}, size of tuples in bytes: {}, number of fields in schema: {}, tbRaw(total number of bytes: {}, "
                   "start of "
                   "current tuple: {}, end of current tuple: {}), tbFormatted(number of tuples: {}, current field offset: {})",
                   (progressTracker.tupleSeparator == "\n") ? "\\n" : progressTracker.tupleSeparator,
                   progressTracker.tupleSizeInBytes,
                   progressTracker.numSchemaFields,
                   progressTracker.numTotalBytesInTBRaw,
                   progressTracker.currentTupleStartTBRaw,
                   progressTracker.currentTupleEndTBRaw,
                   progressTracker.numTuplesInTBFormatted,
                   progressTracker.currentFieldOffsetTBFormatted);
    }

    /// Getter & Setter
    uint64_t getNumSchemaFields() const { return this->numSchemaFields; }
    NES::Memory::TupleBuffer& getTupleBufferFormatted() { return this->tupleBufferFormatted; }
    void setNumberOfTuplesInTBFormatted() { this->tupleBufferFormatted.setNumberOfTuples(numTuplesInTBFormatted); }
    const std::string& getTupleSeparator() { return this->tupleSeparator; }

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

SourceParserCSV::SourceParserCSV(SchemaPtr schema, std::string tupleSeparator, std::string delimiter)
    : schema(std::move(schema))
    , fieldDelimiter(std::move(delimiter))
    , progressTracker(
          std::make_unique<ProgressTracker>(std::move(tupleSeparator), this->schema->getSchemaSizeInBytes(), this->schema->getSize()))
{
    const auto defaultPhysicalTypeFactory = DefaultPhysicalTypeFactory();
    for (const AttributeFieldPtr& field : this->schema->fields)
    {
        auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
        physicalTypes.push_back(physicalField);
    }
}
SourceParserCSV::~SourceParserCSV() = default;

void writeBasicTypeToTupleBuffer(std::string inputString, int8_t* fieldPointer, const BasicPhysicalType& basicPhysicalType)
{
    if (inputString.empty())
    {
        throw Exceptions::RuntimeException("Input string for parsing is empty");
    }

    try
    {
        switch (basicPhysicalType.nativeType)
        {
            case NES::BasicPhysicalType::NativeType::INT_8: {
                const auto value = static_cast<int8_t>(std::stoi(inputString));
                *fieldPointer = value;
                break;
            }
            case NES::BasicPhysicalType::NativeType::INT_16: {
                const auto value = static_cast<int16_t>(std::stol(inputString));
                const auto int16Ptr = reinterpret_cast<int16_t*>(fieldPointer);
                *int16Ptr = value;
                break;
            }
            case NES::BasicPhysicalType::NativeType::INT_32: {
                const auto value = static_cast<int32_t>(std::stol(inputString));
                auto int32Ptr = reinterpret_cast<int32_t*>(fieldPointer);
                *int32Ptr = value;
                break;
            }
            case NES::BasicPhysicalType::NativeType::INT_64: {
                const auto value = static_cast<int64_t>(std::stoll(inputString));
                auto int64Ptr = reinterpret_cast<int64_t*>(fieldPointer);
                *int64Ptr = value;
                break;
            }
            case NES::BasicPhysicalType::NativeType::UINT_8: {
                const auto value = static_cast<uint8_t>(std::stoi(inputString));
                auto uint8Ptr = reinterpret_cast<uint8_t*>(fieldPointer);
                *uint8Ptr = value;
                break;
            }
            case NES::BasicPhysicalType::NativeType::UINT_16: {
                const auto value = static_cast<uint16_t>(std::stoul(inputString));
                auto uint16Ptr = reinterpret_cast<uint16_t*>(fieldPointer);
                *uint16Ptr = value;
                break;
            }
            case NES::BasicPhysicalType::NativeType::UINT_32: {
                auto value = static_cast<uint32_t>(std::stoul(inputString));
                auto uint32Ptr = reinterpret_cast<uint32_t*>(fieldPointer);
                *uint32Ptr = value;
                break;
            }
            case NES::BasicPhysicalType::NativeType::UINT_64: {
                auto value = static_cast<uint64_t>(std::stoull(inputString));
                auto uint64Ptr = reinterpret_cast<uint64_t*>(fieldPointer);
                *uint64Ptr = value;
                break;
            }
            case NES::BasicPhysicalType::NativeType::FLOAT: {
                NES::Util::findAndReplaceAll(inputString, ",", ".");
                auto value = static_cast<float>(std::stof(inputString));
                auto floatPtr = reinterpret_cast<float*>(fieldPointer);
                *floatPtr = value;
                break;
            }
            case NES::BasicPhysicalType::NativeType::DOUBLE: {
                auto value = static_cast<double>(std::stod(inputString));
                auto doublePtr = reinterpret_cast<double*>(fieldPointer);
                *doublePtr = value;
                break;
            }
            case NES::BasicPhysicalType::NativeType::CHAR: {
                ///verify that only a single char was transmitted
                if (inputString.size() > 1)
                {
                    NES_FATAL_ERROR("SourceFormatIterator::mqttMessageToNESBuffer: Received non char Value for CHAR Field {}", inputString);
                    throw std::invalid_argument("Value " + inputString + " is not a char");
                }
                char value = inputString.at(0);
                auto charPtr = reinterpret_cast<char*>(fieldPointer);
                *charPtr = value;
                break;
            }
            case NES::BasicPhysicalType::NativeType::BOOLEAN: {
                ///verify that a valid bool was transmitted (valid{true,false,0,1})
                bool value = !strcasecmp(inputString.c_str(), "true") || !strcasecmp(inputString.c_str(), "1");
                if (!value)
                {
                    if (strcasecmp(inputString.c_str(), "false") && strcasecmp(inputString.c_str(), "0"))
                    {
                        NES_FATAL_ERROR(
                            "Parser::writeFieldValueToTupleBuffer: Received non boolean value for BOOLEAN field: {}", inputString.c_str());
                        throw std::invalid_argument("Value " + inputString + " is not a boolean");
                    }
                }
                auto boolPtr = reinterpret_cast<bool*>(fieldPointer);
                *boolPtr = value;
                break;
            }
            case NES::BasicPhysicalType::NativeType::UNDEFINED:
                NES_FATAL_ERROR("Parser::writeFieldValueToTupleBuffer: Field Type UNDEFINED");
        }
    }
    catch (const std::exception& e)
    {
        NES_ERROR("Failed to convert inputString to desired NES data type. Error: {} for inputString {}", e.what(), inputString);
    }
}

void SourceParserCSV::parseTupleBufferRaw(
    const NES::Memory::TupleBuffer& tbRaw,
    NES::Memory::AbstractBufferProvider& bufferProvider,
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
        parseStringTupleToTBFormatted(completeTuple.str(), bufferProvider);
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
        parseStringTupleToTBFormatted(currentTuple, bufferProvider);

        progressTracker->progressOneTuple();
    }

    progressTracker->setNumberOfTuplesInTBFormatted();
    NES_TRACE("emitting parsed tuple buffer with {} tuples.", progressTracker->numTuplesInTBFormatted);

    /// Emit the current TBF, even if there is only a single tuple (there is at least one) in it.
    emitFunction(progressTracker->getTupleBufferFormatted(), /* add metadata */ true);
    progressTracker->handleResidualBytes(tbRaw);
}

void SourceParserCSV::parseStringTupleToTBFormatted(
    const std::string_view inputString, NES::Memory::AbstractBufferProvider& bufferProvider) const
{
    std::vector<std::string> values;
    try
    {
        values = NES::Util::splitWithStringDelimiter<std::string>(inputString, fieldDelimiter);
    }
    catch (std::exception e)
    {
        throw CSVParsingError(fmt::format("An error occurred while splitting delimiter. ERROR: {}", strerror(errno)));
    }

    /// Iterate over all (potentially except the first) fields, parse the string values and write the formatted data into the TBF.
    for (size_t currentFieldNumber = 0; const auto& currentVal : values)
    {
        if (auto basicType = dynamic_pointer_cast<BasicPhysicalType>(physicalTypes.at(currentFieldNumber)))
        {
            writeBasicTypeToTupleBuffer(currentVal, progressTracker->getCurrentFieldPointer(), *basicType);
        }
        else
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
            auto childBufferIndexPointer = reinterpret_cast<uint32_t*>(progressTracker->getCurrentFieldPointer());
            *childBufferIndexPointer = index;
        }
        progressTracker->currentFieldOffsetTBFormatted += this->fieldSizes.at(currentFieldNumber);
        ++currentFieldNumber;
    }
}

std::unique_ptr<SourceParser> SourceParserGeneratedRegistrar::RegisterSourceParserCSV(
    std::shared_ptr<Schema> schema, std::string tupleSeparator, std::string fieldDelimiter)
{
    return std::make_unique<SourceParserCSV>(std::move(schema), std::move(tupleSeparator), std::move(fieldDelimiter));
}

}
