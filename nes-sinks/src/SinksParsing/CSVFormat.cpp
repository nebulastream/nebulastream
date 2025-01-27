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

#include <iostream>
#include <utility>

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <MemoryLayout/MemoryLayout.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SinksParsing/CSVFormat.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/VariableSizedDataType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>

namespace NES::Sinks
{

CSVFormat::CSVFormat(std::shared_ptr<Schema> schema, bool addTimestamp) : schema(std::move(schema)), addTimestamp(addTimestamp)
{
}

std::string CSVFormat::getFormattedSchema() const
{
    if (addTimestamp)
    {
        return schema->toString("", ", ", ", timestamp\n");
    }
    return schema->toString("", ", ", "\n");
}

constexpr auto replaceNewlines(const std::string_view input, const std::string_view replacement)
{
    std::string result;
    result.reserve(input.size());

    for (const char c : input)
    {
        if (c == '\n')
        {
            result += replacement;
        }
        else
        {
            result += c;
        }
    }
    return result;
}

std::string CSVFormat::getFormattedBuffer(const Memory::TupleBuffer& inputBuffer)
{
    std::string bufferContent;
    if (addTimestamp)
    {
        auto timestamp = duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        schema->removeField(AttributeField::create("timestamp", DataTypeFactory::createType(BasicType::UINT64)));
        bufferContent = tupleBufferToFormattedCSVString(inputBuffer, schema);
        std::string repReg = "," + std::to_string(timestamp) + "\n";
        bufferContent = replaceNewlines(bufferContent, repReg);
        schema->addField("timestamp", BasicType::UINT64);
    }
    else
    {
        bufferContent = tupleBufferToFormattedCSVString(inputBuffer, schema);
    }
    return bufferContent;
}

std::string CSVFormat::tupleBufferToFormattedCSVString(Memory::TupleBuffer tbuffer, const SchemaPtr& schema)
{
    std::stringstream ss;
    auto numberOfTuples = tbuffer.getNumberOfTuples();
    auto* buffer = tbuffer.getBuffer<char>();
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    for (uint64_t i = 0; i < numberOfTuples; i++)
    {
        uint64_t offset = 0;
        for (uint64_t j = 0; j < schema->getFieldCount(); j++)
        {
            auto field = schema->getFieldByIndex(j);
            auto dataType = field->getDataType();
            auto physicalType = physicalDataTypeFactory.getPhysicalType(dataType);
            auto fieldSize = physicalType->size();
            std::string str;
            auto indexInBuffer = buffer + offset + i * schema->getSchemaSizeInBytes();

            /// handle variable-length field
            if (NES::Util::instanceOf<VariableSizedDataType>(dataType))
            {
                NES_DEBUG("trying to read the variable length TEXT field: {} from the tuple buffer", field->toString());

                /// read the child buffer index from the tuple buffer
                auto childIdx = *reinterpret_cast<const uint32_t*>(indexInBuffer);
                str = Memory::MemoryLayouts::readVarSizedData(tbuffer, childIdx);
            }
            else
            {
                str = physicalType->convertRawToString(indexInBuffer);
            }

            ss << str;
            if (j < schema->getFieldCount() - 1)
            {
                ss << ",";
            }
            offset += fieldSize;
        }
        ss << std::endl;
    }
    return ss.str();
}

std::ostream& operator<<(std::ostream& out, const CSVFormat& format)
{
    return out << fmt::format("CSVFormat(Schema: {}, addTimeStamp: {}", format.schema->toString(), format.addTimestamp);
}

}
