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

#include <ResultChecker/ResultFile.hpp>

#include <filesystem>
#include <fstream>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
Schema<UnqualifiedUnboundField, Ordered> parseFieldNames(const std::string_view fieldNamesRawLine)
{
    /// Assumes the field and type to be similar to window$val_i8_i8:INT32:IS_NULLABLE, window$val_i8_i8_plus_1:INT16:NOT_NULLABLE
    auto fields
        = std::ranges::split_view(fieldNamesRawLine, ',')
        | std::views::transform([](auto splitNameAndType) { return std::string_view(splitNameAndType.begin(), splitNameAndType.end()); })
        | std::views::filter([](const auto& stringViewSplit) { return !stringViewSplit.empty(); })
        | std::views::transform(
              [](const auto& field)
              {
                  /// At this point, we have a field and tpye separated by a colon, e.g., "window$val_i8_i8:INT32"
                  /// We need to split the fieldName and type by the colon, store the field name and type in a vector.
                  /// After that, we can trim the field name and type and store it in the fields vector.
                  /// "window$val_i8_i8:INT32:IS_NULLABLE " -> ["window$val_i8_i8", "INT32 ", " IS_NULLABLE"] -> {"window$val_i8_i8", INT32, NOT_NULLABLE}
                  const auto [nameTrimmed, typeTrimmed, isNullable]
                      = [](const std::string_view field) -> std::tuple<std::string_view, std::string_view, DataType::NULLABLE>
                  {
                      std::vector<std::string_view> fieldAndTypeVector;
                      for (const auto subrange : std::ranges::split_view(field, ':'))
                      {
                          fieldAndTypeVector.emplace_back(trimWhiteSpaces(std::string_view(subrange)));
                      }
                      INVARIANT(
                          fieldAndTypeVector.size() == 3, "Field and type pairs should always be pairs of a key, a value and isNullable");

                      const auto isNullableString = fieldAndTypeVector.at(2);
                      const auto isNullable = magic_enum::enum_cast<DataType::NULLABLE>(isNullableString);
                      if (not isNullable)
                      {
                          throw SLTUnexpectedToken("Unknown nullable: {}", isNullableString);
                      }
                      return std::make_tuple(fieldAndTypeVector.at(0), fieldAndTypeVector.at(1), isNullable.value());
                  }(field);
                  DataType dataType;
                  if (auto type = magic_enum::enum_cast<DataType::Type>(typeTrimmed); type.has_value())
                  {
                      dataType = DataTypeProvider::provideDataType(type.value(), isNullable);
                  }
                  else if (toLowerCase(typeTrimmed) == "varsized")
                  {
                      dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED, isNullable);
                  }
                  else
                  {
                      throw SLTUnexpectedToken("Unknown basic type: {}", typeTrimmed);
                  }
                  /// The header holds the name with the case that the sink wrote, so it parses as a quoted identifier to keep that case.
                  return UnqualifiedUnboundField{Identifier::parse(fmt::format("\"{}\"", nameTrimmed)), dataType};
              });
    return fields | std::ranges::to<Schema<UnqualifiedUnboundField, Ordered>>();
}
}

std::optional<QueryResult> loadQueryResult(const std::filesystem::path& resultFilePath)
{
    NES_DEBUG("Loading query result from: {}", resultFilePath);
    std::ifstream resultFile(resultFilePath);
    if (!resultFile)
    {
        NES_ERROR("Failed to open result file: {}", resultFilePath);
        return std::nullopt;
    }

    QueryResult result;
    std::string firstLine;
    if (!std::getline(resultFile, firstLine))
    {
        NES_ERROR("Result file is empty", resultFilePath);
        return std::nullopt;
    }

    result.schema = parseFieldNames(firstLine);

    while (std::getline(resultFile, firstLine))
    {
        result.result.push_back(firstLine);
    }
    return result;
}

}
