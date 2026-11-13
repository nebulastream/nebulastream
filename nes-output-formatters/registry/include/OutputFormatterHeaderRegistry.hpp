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

#include <functional>
#include <optional>
#include <string>

#include <DataTypes/UnboundField.hpp>
#include <OutputFormatters/OutputFormatterDescriptor.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/RuntimeRegistry.hpp>

namespace NES
{

/// Maps an output formatter type name to the header the format writes in front of its rows (see e.g. CSVOutputFormatter::getHeader),
/// or nullopt for a format without one. The header depends only on the schema and the formatter config, so it needs no formatter instance.
/// Entries self-register at static initialization time via generated glue TUs
/// (see create_runtime_registry(OutputFormatterHeader ...) in nes-output-formatters/CMakeLists.txt).
using OutputFormatterHeaderFn
    = std::function<std::optional<std::string>(const Schema<UnqualifiedUnboundField, Ordered>&, const OutputFormatterDescriptor&)>;

class OutputFormatterHeaderRegistry
    : public RuntimeRegistry<OutputFormatterHeaderRegistry, std::string, OutputFormatterHeaderFn, /*CaseSensitive*/ false>
{
public:
    static OutputFormatterHeaderRegistry& instance();
};

}
