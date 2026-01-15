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
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <ostream>
#include <ranges>
#include <sstream>
#include <string>
#include <utility>

#include <Runtime/VariableSizedAccess.hpp>
#include <fmt/base.h>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <ErrorHandling.hpp>
#include <val.hpp>
#include <val_ptr.hpp>
#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>

namespace NES
{

/// The output formatter is responsible for converting a Record into a string of a given Output Format like CSV or JSON
/// Output formatters are stateless, meaning that they must be able to produce valid output in a streaming fashion.
/// Output formatters are used by the OutputFormatterBufferRef during the last emit before a sink.
class OutputFormatter
{
public:
    explicit OutputFormatter(const size_t numberOfFields) : numberOfFields(numberOfFields) { }

    virtual ~OutputFormatter() noexcept = default;

    /// Format the Fields contents into a string and write the string bytes into the field pointer adress. Should the string be larger than
    /// remaining size, the method will not write anything and return nullopt.
    /// Otherwise, it will return the number of bytes written to the address.
    [[nodiscard]] virtual nautilus::val<size_t> getFormattedValue(
        VarVal value,
        const DataType& fieldType,
        const nautilus::static_val<uint64_t>& fieldIndex,
        const nautilus::val<int8_t*>& fieldPointer,
        const nautilus::val<uint64_t>& remainingSize) const = 0;

    virtual std::ostream& toString(std::ostream&) const = 0;

    friend std::ostream& operator<<(std::ostream& os, const OutputFormatter& obj);

protected:
    /// Number of fields in the output schema
    size_t numberOfFields;
};

}

template <std::derived_from<NES::OutputFormatter> OutputFormatter>
struct fmt::formatter<OutputFormatter> : fmt::ostream_formatter
{
};
