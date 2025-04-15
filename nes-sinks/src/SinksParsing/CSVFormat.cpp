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

#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <ranges>
#include <span>
#include <sstream>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <API/Schema.hpp>
#include <MemoryLayout/MemoryLayout.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SinksParsing/CSVFormat.hpp>
#include <Util/Common.hpp>
#include <Util/Overloaded.hpp>
#include <ErrorHandling.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/VariableSizedDataPhysicalType.hpp>

namespace NES::Sinks
{

CSVFormat::CSVFormat(std::shared_ptr<Schema> inputSchema) : schema(std::move(inputSchema))
{
    PRECONDITION(schema->getFieldCount() != 0, "Formatter expected a non-empty schema");
    size_t offset = 0;
    for (const auto& field : *schema)
    {
        const DefaultPhysicalTypeFactory factory;
        auto physicalType = factory.getPhysicalType(field->getDataType());
        PRECONDITION(
            Util::instanceOf<BasicPhysicalType>(physicalType) || Util::instanceOf<VariableSizedDataPhysicalType>(physicalType),
            "Formatter can only handle basic and variable size physical types");

        formattingContext.nullable.push_back(field->isNullable());
        formattingContext.offsets.push_back(offset);
        offset += physicalType->getSizeInBytes();
        if (auto basicType = std::dynamic_pointer_cast<BasicPhysicalType>(physicalType))
        {
            formattingContext.physicalTypes.emplace_back(std::move(basicType));
        }
        else
        {
            formattingContext.physicalTypes.emplace_back(std::dynamic_pointer_cast<VariableSizedDataPhysicalType>(physicalType));
        }
    }
    formattingContext.schemaSizeInBytes = schema->getSchemaSizeInBytes();
}

std::string CSVFormat::getFormattedSchema() const
{
    return schema->toString("", ", ", "\n");
}


std::string CSVFormat::getFormattedBuffer(const Memory::TupleBuffer& inputBuffer) const
{
    return tupleBufferToFormattedCSVString(inputBuffer, formattingContext);
}

std::string CSVFormat::tupleBufferToFormattedCSVString(const Memory::TupleBuffer& inputBuffer, const FormattingContext& formattingContext)
{
    std::stringstream bufString;

    const auto numberOfTuples = inputBuffer.getNumberOfTuples();
    const auto bufSpan = std::span(inputBuffer.getBuffer<char>(), numberOfTuples * formattingContext.schemaSizeInBytes);

    for (size_t i = 0; i < numberOfTuples; i++)
    {
        auto tuple = bufSpan.subspan(i * formattingContext.schemaSizeInBytes, formattingContext.schemaSizeInBytes);
        std::vector<std::string> fields;
        fields.reserve(formattingContext.offsets.size());

        for (size_t index = 0; index < formattingContext.offsets.size(); ++index)
        {
            const auto offset = formattingContext.offsets[index];
            const auto isNullable = formattingContext.nullable[index];
            const auto& physicalType = formattingContext.physicalTypes[index];

            std::string field = std::visit(
                Overloaded{
                    [&](const std::shared_ptr<VariableSizedDataPhysicalType>&) -> std::string
                    {
                        if (const auto nullOffset = offset + sizeof(uint32_t); isNullable && tuple[nullOffset] != 0)
                        {
                            return {};
                        }
                        const auto childIdx = *reinterpret_cast<const uint32_t*>(&tuple[offset]);
                        return Memory::MemoryLayouts::readVarSizedData(inputBuffer, childIdx);
                    },
                    [&](const std::shared_ptr<BasicPhysicalType>& type) -> std::string
                    {
                        if (const auto nullOffset = offset + type->getRawSizeInBytes(); isNullable && tuple[nullOffset] != 0)
                        {
                            return {};
                        }
                        return type->convertRawToString(&tuple[offset]);
                    },
                },
                physicalType);

            fields.emplace_back(std::move(field));
        }

        bufString << fmt::format("{}\n", fmt::join(fields, ","));
    }
    return bufString.str();
}


std::ostream& operator<<(std::ostream& out, const CSVFormat& format)
{
    return out << fmt::format("CSVFormat(Schema: {})", format.schema->toString());
}

}
