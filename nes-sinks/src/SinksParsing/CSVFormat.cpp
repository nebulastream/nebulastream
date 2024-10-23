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
#include <regex>
#include <utility>

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Common/DataTypes/TextType.hpp>
#include <MemoryLayout/MemoryLayout.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SinksParsing/CSVFormat.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>

namespace NES::Sinks
{

CSVFormat::CSVFormat(std::shared_ptr<Schema> schema, bool addTimestamp) : schema(std::move(schema)), addTimestamp(addTimestamp)
{
}

std::string CSVFormat::getFormattedSchema() const
{
    std::stringstream ss;
    for (auto& attributeField : schema->fields)
    {
        ss << attributeField->toString() << ", ";
    }
    ss.seekp(-1, std::ios_base::end);
    ss << std::endl;

    if (addTimestamp)
    {
        ss << Util::trimWhiteSpaces(ss.str());
        ss << ", timestamp\n";
    }
    return ss.str();
}

std::string CSVFormat::getFormattedBuffer(Memory::TupleBuffer& inputBuffer)
{
    std::string bufferContent;
    if (addTimestamp)
    {
        auto timestamp = duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        schema->removeField(AttributeField::create("timestamp", DataTypeFactory::createType(BasicType::UINT64)));
        bufferContent = printTupleBufferAsCSV(inputBuffer, schema);
        std::string repReg = "," + std::to_string(timestamp) + "\n";
        bufferContent = std::regex_replace(bufferContent, std::regex(R"(\n)"), repReg);
        schema->addField("timestamp", BasicType::UINT64);
    }
    else
    {
        bufferContent = printTupleBufferAsCSV(inputBuffer, schema);
    }
    return bufferContent;
}

std::string CSVFormat::printTupleBufferAsCSV(Memory::TupleBuffer tbuffer, const SchemaPtr& schema)
{
    std::stringstream ss;
    auto numberOfTuples = tbuffer.getNumberOfTuples();
    auto* buffer = tbuffer.getBuffer<char>();
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    for (uint64_t i = 0; i < numberOfTuples; i++)
    {
        uint64_t offset = 0;
        for (uint64_t j = 0; j < schema->getSize(); j++)
        {
            auto field = schema->get(j);
            auto dataType = field->getDataType();
            auto physicalType = physicalDataTypeFactory.getPhysicalType(dataType);
            auto fieldSize = physicalType->size();
            std::string str;
            auto indexInBuffer = buffer + offset + i * schema->getSchemaSizeInBytes();

            /// handle variable-length field
            if (NES::Util::instanceOf<TextType>(dataType))
            {
                NES_DEBUG("trying to read the variable length TEXT field: {} from the tuple buffer", field->toString());

                /// read the child buffer index from the tuple buffer
                auto childIdx = *reinterpret_cast<uint32_t const*>(indexInBuffer);
                str = Memory::MemoryLayouts::readVarSizedData(tbuffer, childIdx);
            }
            else
            {
                str = physicalType->convertRawToString(indexInBuffer);
            }

            ss << str;
            if (j < schema->getSize() - 1)
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
