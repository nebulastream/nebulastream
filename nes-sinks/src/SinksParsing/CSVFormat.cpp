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

#include <ranges>
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

CSVFormat::CSVFormat(std::shared_ptr<Schema> pSchema, bool addTimestamp) : schema(std::move(pSchema)), addTimestamp(addTimestamp)
{
    PRECONDITION(schema->getFieldCount() != 0, "Formatter expected a non-empty schema");
    DefaultPhysicalTypeFactory factory;
    size_t offset = 0;
    for (const auto& f : *schema)
    {
        auto physicalType = factory.getPhysicalType(f->getDataType());
        formattingContext.offsets.push_back(offset);
        offset += physicalType->size();
        bool isVariableSize = Util::instanceOf<VariableSizedDataType>(physicalType);
        formattingContext.physicalTypes.emplace_back(physicalType, isVariableSize);
    }
    formattingContext.schemaSizeInBytes = schema->getSchemaSizeInBytes();
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
        bufferContent = tupleBufferToFormattedCSVString(inputBuffer, formattingContext);
        std::string repReg = "," + std::to_string(timestamp) + "\n";
        bufferContent = replaceNewlines(bufferContent, repReg);
        schema->addField("timestamp", BasicType::UINT64);
    }
    else
    {
        bufferContent = tupleBufferToFormattedCSVString(inputBuffer, formattingContext);
    }
    return bufferContent;
}


std::string CSVFormat::tupleBufferToFormattedCSVString(Memory::TupleBuffer tbuffer, const FormattingContext& formattingContext)
{
    std::stringstream ss;
    auto numberOfTuples = tbuffer.getNumberOfTuples();
    auto buffer = std::span(tbuffer.getBuffer<char>(), numberOfTuples * formattingContext.schemaSizeInBytes);
    for (size_t i = 0; i < numberOfTuples; i++)
    {
        auto tuple = buffer.subspan(i * formattingContext.schemaSizeInBytes, formattingContext.schemaSizeInBytes);
        if (formattingContext.physicalTypes[0].second)
        {
            auto childIdx = *reinterpret_cast<uint32_t const*>(&tuple[formattingContext.offsets[0]]);
            ss << Memory::MemoryLayouts::readVarSizedData(tbuffer, childIdx);
        }
        else
        {
            ss << formattingContext.physicalTypes[0].first->convertRawToString(&tuple[formattingContext.offsets[0]]);
        }

        for (const auto& [offset, typeAndVariable] :
             std::views::zip(formattingContext.offsets, formattingContext.physicalTypes) | std::views::drop(1))
        {
            ss << ",";
            const auto& [type, isVariableSize] = typeAndVariable;
            if (isVariableSize)
            {
                const auto childIdx = *reinterpret_cast<uint32_t const*>(&tuple[offset]);
                ss << Memory::MemoryLayouts::readVarSizedData(tbuffer, childIdx);
            }
            else
            {
                ss << type->convertRawToString(&tuple[offset]);
            }
        }

        ss << '\n';
    }
    return ss.str();
}

std::ostream& operator<<(std::ostream& out, const CSVFormat& format)
{
    return out << fmt::format("CSVFormat(Schema: {}, addTimeStamp: {}", format.schema->toString(), format.addTimestamp);
}

}
