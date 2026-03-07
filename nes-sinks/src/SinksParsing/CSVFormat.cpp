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

#include <SinksParsing/CSVFormat.hpp>

#include <bit>
#include <cstddef>
#include <cstdint>
#include <ostream>
#include <ranges>
#include <span>
#include <sstream>
#include <string>
#include <DataTypes/Schema.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/VariableSizedAccess.hpp>
#include <SinksParsing/Format.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <magic_enum/magic_enum.hpp>

#include <ErrorHandling.hpp>

namespace NES
{
CSVFormat::CSVFormat(const Schema& schema) : CSVFormat(schema, false)
{
}

CSVFormat::CSVFormat(const Schema& pSchema, const bool escapeStrings) : Format(pSchema), escapeStrings(escapeStrings)
{
    PRECONDITION(schema.getNumberOfFields() != 0, "Formatter expected a non-empty schema");
    size_t offset = 0;
    for (const auto& field : schema.getFields())
    {
        const auto physicalType = field.dataType;
        formattingContext.offsets.push_back(offset);
        offset += physicalType.getSizeInBytesWithNull();
        formattingContext.physicalTypes.emplace_back(physicalType);
    }
    formattingContext.schemaSizeInBytes = schema.getSizeOfSchemaInBytes();
}

std::string CSVFormat::getFormattedBuffer(const TupleBuffer& inputBuffer) const
{
    return tupleBufferToFormattedCSVString(inputBuffer, formattingContext);
}

std::string CSVFormat::tupleBufferToFormattedCSVString(TupleBuffer tbuffer, const FormattingContext& formattingContext) const
{
    std::stringstream ss;
    const auto numberOfTuples = tbuffer.getNumberOfTuples();
    const auto buffer = tbuffer.getAvailableMemoryArea().subspan(0, numberOfTuples * formattingContext.schemaSizeInBytes);
    for (size_t i = 0; i < numberOfTuples; i++)
    {
        auto tuple = buffer.subspan(i * formattingContext.schemaSizeInBytes, formattingContext.schemaSizeInBytes);
        auto fields
            = std::views::iota(static_cast<size_t>(0), formattingContext.offsets.size())
            | std::views::transform(
                  [&formattingContext, &tuple, &tbuffer, copyOfEscapeStrings = escapeStrings](const auto& index) -> std::string
                  {
                      const auto physicalType = formattingContext.physicalTypes[index];
                      auto fieldValueStart = tuple.subspan(formattingContext.offsets[index], physicalType.getSizeInBytesWithNull());
                      if (physicalType.nullable)
                      {
                          /// Convert byte to bool: true if byte is non-zero, false otherwise
                          const bool isNull = static_cast<bool>(std::to_integer<int>(fieldValueStart[0]));
                          fieldValueStart = fieldValueStart.subspan(1);
                          if (isNull)
                          {
                              /// We need to write null, as otherwise, we can not detect a single null in our output
                              return "NULL";
                          }
                      }

                      if (physicalType.type == DataType::Type::VARSIZED)
                      {
                          const auto base = formattingContext.offsets[index] + physicalType.nullable;
                          const auto* indexPtr = std::bit_cast<const uint32_t*>(&tuple[base + offsetof(VariableSizedAccess, index)]);
                          const auto* offsetPtr = std::bit_cast<const uint32_t*>(&tuple[base + offsetof(VariableSizedAccess, offset)]);
                          const auto* sizePtr = std::bit_cast<const uint64_t*>(&tuple[base + offsetof(VariableSizedAccess, size)]);

                          const VariableSizedAccess variableSizedAccess{
                              VariableSizedAccess::Index(*indexPtr),
                              VariableSizedAccess::Offset(*offsetPtr),
                              VariableSizedAccess::Size(*sizePtr)};
                          auto varSizedData = readVarSizedDataAsString(tbuffer, variableSizedAccess);
                          if (copyOfEscapeStrings)
                          {
                              return "\"" + varSizedData + "\"";
                          }
                          return varSizedData;
                      }
                      return physicalType.formattedBytesToString(fieldValueStart.data());
                  });
        ss << fmt::format("{}\n", fmt::join(fields, ","));
    }
    return ss.str();
}

std::ostream& operator<<(std::ostream& out, const CSVFormat& format)
{
    return out << fmt::format("CSVFormat(Schema: {})", format.schema);
}

}
