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
#include <ostream>
#include <string>
#include <unordered_map>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <OutputFormatters/OutputFormatter.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Util/Logger/Formatter.hpp>
#include <static.hpp>
#include <val_arith.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{

/// Thin OutputFormatter shim for Arrow format.
/// The actual buffer writing is handled by ArrowBufferRef (a TupleBufferRef subclass).
/// This class exists only for plugin registry integration (discovery, validation, config).
/// Its writeFormattedValue() should never be called — LowerSchemaProvider routes Arrow
/// format to ArrowBufferRef instead of OutputFormatterBufferRef.
class ArrowOutputFormatter : public OutputFormatter
{
public:
    explicit ArrowOutputFormatter(const std::vector<Record::RecordFieldIdentifier>& fieldNames);

    [[nodiscard]] nautilus::val<uint64_t> writeFormattedValue(
        const VarVal& value,
        const DataType& fieldType,
        const nautilus::static_val<uint64_t>& fieldIndex,
        const nautilus::val<int8_t*>& fieldPointer,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider) const override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    std::ostream& toString(std::ostream& os) const override { return os << *this; }

    friend std::ostream& operator<<(std::ostream& out, const ArrowOutputFormatter& format);
};

}

namespace NES::OutputFormatterConfig
{
struct ConfigParametersArrow
{
    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap();
};
}

FMT_OSTREAM(NES::OutputFormatter);
