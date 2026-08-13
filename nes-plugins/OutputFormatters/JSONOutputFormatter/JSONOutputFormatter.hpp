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
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <vector>
#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigValue.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <OutputFormatters/OutputFormatter.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Logger/Formatter.hpp>
#include <ErrorHandling.hpp>
#include <OutputFormatterRegistry.hpp>
#include <static.hpp>
#include <val_arith.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{

/// Formatter-defined config struct: the JSON output formatter declares no config parameters, but
/// the (empty) struct still travels through the OutputFormatterDescriptor and its registry entry
/// so that descriptors are handled uniformly.
struct JSONOutputFormatterConfig
{
    static std::expected<JSONOutputFormatterConfig, Exception> fromConfig(const InstantiatedConfig& config);
};

class JSONOutputFormatter : public OutputFormatter
{
public:
    static constexpr std::string_view NAME = "JSON";

    explicit JSONOutputFormatter(const std::vector<Record::RecordFieldIdentifier>& fieldNames);

    [[nodiscard]] nautilus::val<uint64_t> writeFormattedValue(
        const VarVal& value,
        const DataType& fieldType,
        const nautilus::static_val<uint64_t>& fieldIndex,
        const nautilus::val<int8_t*>& fieldPointer,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider) const override;

    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

    /// Registry entry (see OutputFormatterRegistry.hpp).
    static std::unique_ptr<OutputFormatter> provideFormatter(OutputFormatterRegistryArguments arguments);

    std::ostream& toString(std::ostream& os) const override { return os << *this; }

    friend std::ostream& operator<<(std::ostream& out, const JSONOutputFormatter& format);

private:
    std::vector<std::string> canonicalFieldNames;
};

}

FMT_OSTREAM(NES::OutputFormatter);
