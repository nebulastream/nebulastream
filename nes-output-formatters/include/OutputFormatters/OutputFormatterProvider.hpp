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

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <vector>
#include <DataTypes/UnboundField.hpp>
#include <Interface/Record.hpp>
#include <OutputFormatters/OutputFormatter.hpp>
#include <OutputFormatters/OutputFormatterDescriptor.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>

namespace NES::OutputFormatterProvider
{
[[nodiscard]] std::shared_ptr<OutputFormatter> provideOutputFormatter(
    const std::string& outputFormatterType,
    const std::vector<Record::RecordFieldIdentifier>& fieldNames,
    const OutputFormatterDescriptor& descriptor);

/// The header that a sink writes once in front of the rows, or nullopt if the format has none.
/// Native output is not formatted, so it has no header either.
[[nodiscard]] std::optional<std::string>
provideHeader(const OutputFormatterDescriptor& descriptor, const Schema<UnqualifiedUnboundField, Ordered>& schema);
}
