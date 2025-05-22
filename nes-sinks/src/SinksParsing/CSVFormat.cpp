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
#include <API/Schema.hpp>
#include <MemoryLayout/MemoryLayout.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SinksParsing/CSVFormat.hpp>
#include <Util/Common.hpp>
#include <Util/Overloaded.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/VariableSizedDataPhysicalType.hpp>

namespace NES::Sinks
{

CSVFormat::CSVFormat(std::shared_ptr<Schema> pSchema) : schema(std::move(pSchema))
{
    PRECONDITION(schema->getFieldCount() != 0, "Formatter expected a non-empty schema");
    const DefaultPhysicalTypeFactory factory;
    size_t offset = 0;
    for (const auto& f : *schema)
    {
        auto physicalType = factory.getPhysicalType(f->getDataType());
        PRECONDITION(
            Util::instanceOf<BasicPhysicalType>(physicalType) || Util::instanceOf<VariableSizedDataPhysicalType>(physicalType),
            "Formatter can only handle basic and variable size physical types");

        formattingContext.offsets.push_back(offset);
        offset += physicalType->size();
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

std::string CSVFormat::getFormattedBuffer(const Memory::TupleBuffer& inputBuffer)
{
    return tupleBufferToFormattedCSVString(inputBuffer, formattingContext);
}

std::string CSVFormat::tupleBufferToFormattedCSVString(Memory::TupleBuffer tbuffer, const FormattingContext& formattingContext)
{
    std::stringstream ss;
    auto numberOfTuples = tbuffer.getNumberOfTuples();
    auto buffer = std::span(tbuffer.getBuffer<char>(), numberOfTuples * formattingContext.schemaSizeInBytes);
    for (size_t i = 0; i < numberOfTuples; i++)
    {
        auto tuple = buffer.subspan(i * formattingContext.schemaSizeInBytes, formattingContext.schemaSizeInBytes);
        auto fields = std::views::iota(static_cast<size_t>(0), formattingContext.offsets.size())
            | std::views::transform(
                          [&](const auto& index)
                          {
                              return std::visit(
                                  Overloaded{
                                      [&](const std::shared_ptr<VariableSizedDataPhysicalType>&)
                                      {
                                          auto childIdx = *reinterpret_cast<const uint32_t*>(&tuple[formattingContext.offsets[index]]);
                                          return Memory::MemoryLayouts::readVarSizedData(tbuffer, childIdx);
                                      },
                                      [&](const std::shared_ptr<BasicPhysicalType>& type)
                                      { return type->convertRawToString(&tuple[formattingContext.offsets[index]]); },
                                  },
                                  formattingContext.physicalTypes[index]);
                          });

        ss << fmt::format("{}\n", fmt::join(fields, ","));
    }
    return ss.str();
}

std::ostream& operator<<(std::ostream& out, const CSVFormat& format)
{
    return out << fmt::format("CSVFormat(Schema: {})", format.schema->toString());
}

}
