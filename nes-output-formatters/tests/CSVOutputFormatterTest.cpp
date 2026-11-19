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

#include <CSVOutputFormatter.hpp>

#include <optional>
#include <ranges>
#include <string>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Interface/Record.hpp>
#include <OutputFormatters/CSVOutputFormatterConfig.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <gtest/gtest.h>

namespace NES
{
namespace
{
using TestSchema = Schema<UnqualifiedUnboundField, Ordered>;

UnqualifiedUnboundField field(std::string name, const DataType::Type type, const DataType::NULLABLE nullable)
{
    return UnqualifiedUnboundField{Identifier::parse(std::move(name)), DataType{type, nullable}};
}

CSVOutputFormatter createFormatter(const TestSchema& schema, std::string headerFormat, std::string fieldDelimiter)
{
    const auto fieldNames = schema
        | std::views::transform([](const auto& field) -> Record::RecordFieldIdentifier { return field.getFullyQualifiedName(); })
        | std::ranges::to<std::vector>();
    return CSVOutputFormatter{
        fieldNames,
        CSVOutputFormatterConfig{
            .quoteStrings = false,
            .fieldDelimiter = std::move(fieldDelimiter),
            .tupleDelimiter = "\n",
            .valueSerializers = "",
            .headerFormat = std::move(headerFormat)}};
}
}

TEST(CSVOutputFormatterTest, DefaultHeaderFormat)
{
    const TestSchema schema{
        field("id", DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE),
        field("value", DataType::Type::FLOAT64, DataType::NULLABLE::IS_NULLABLE)};
    const auto formatter = createFormatter(schema, "{NAME}:{TYPE}:{NULLABLE}", ",");

    EXPECT_EQ(formatter.getHeader(schema), std::optional<std::string>{"ID:UINT64:NOT_NULLABLE,VALUE:FLOAT64:IS_NULLABLE\n"});
}

TEST(CSVOutputFormatterTest, CaseSensitiveNamesAreQuoted)
{
    const TestSchema schema{field(R"("MixedCase")", DataType::Type::INT32, DataType::NULLABLE::NOT_NULLABLE)};
    const auto formatter = createFormatter(schema, "{NAME}:{TYPE}:{NULLABLE}", ",");

    EXPECT_EQ(
        formatter.getHeader(schema),
        std::optional<std::string>{R"("MixedCase":INT32:NOT_NULLABLE)"
                                   "\n"});
}

TEST(CSVOutputFormatterTest, CustomHeaderFormatAndFieldDelimiter)
{
    const TestSchema schema{
        field("a", DataType::Type::BOOLEAN, DataType::NULLABLE::NOT_NULLABLE),
        field("b", DataType::Type::VARSIZED, DataType::NULLABLE::IS_NULLABLE)};
    const auto formatter = createFormatter(schema, "{TYPE} {NAME}", "|");

    EXPECT_EQ(formatter.getHeader(schema), std::optional<std::string>{"BOOLEAN A|VARSIZED B\n"});
}
}
