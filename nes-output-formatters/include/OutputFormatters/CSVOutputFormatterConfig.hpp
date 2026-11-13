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

#pragma once
#include <cstdint>
#include <expected>
#include <string>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

/// The header that the CSV output formatter writes once in front of the rows. Fields are separated by the field delimiter,
/// and the header ends with the tuple delimiter, like a row.
enum class CSVHeaderFormat : uint8_t
{
    /// No header, the output holds only rows.
    NONE,
    /// The field names, e.g., `ID,VALUE`.
    NAME,
    /// Name, type, and nullability of each field, e.g., `ID:UINT64:NOT_NULLABLE,VALUE:FLOAT64:IS_NULLABLE`.
    NAME_TYPE_NULLABLE,
};

/// Formatter-defined config struct: instantiated from the generic config by the
/// OutputFormatterConfig registry entry, carried through the OutputFormatterDescriptor as
/// std::any, and serialized via reflection of exactly this struct (all members are reflectable).
struct CSVOutputFormatterConfig
{
    bool quoteStrings;
    std::string fieldDelimiter;
    std::string tupleDelimiter;
    std::string valueSerializers;
    CSVHeaderFormat headerFormat;

    static std::expected<CSVOutputFormatterConfig, Exception> fromConfig(const InstantiatedConfig& config);
};
}
