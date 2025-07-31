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

#include <memory>
#include <ostream>
#include <DataTypes/DataType.hpp>
#include <Identifiers/Identifier.hpp>
#include <Util/Logger/Formatter.hpp>


namespace NES
{

namespace detail
{
struct ErasedLogicalOperator;
}
template <typename Checked>
struct TypedLogicalOperator;
using LogicalOperator = TypedLogicalOperator<NES::detail::ErasedLogicalOperator>;

struct Field
{
    Field();
    ~Field();
    Field(const LogicalOperator& producedBy, Identifier name, DataType dataType);
    Field(const LogicalOperator& producedBy, Identifier name, DataType::Type dataType);
    Field(const Field& other);
    Field(Field&& other) noexcept;
    Field& operator=(const Field& other);
    Field& operator=(Field&& other) noexcept;

    friend std::ostream& operator<<(std::ostream& os, const Field& field);
    bool operator==(const Field&) const = default;

    [[nodiscard]] const LogicalOperator& getProducedBy() const;
    [[nodiscard]] Identifier getLastName() const { return name; }
    [[nodiscard]] DataType getDataType() const { return dataType; }

private:
    /// I believe we currently need this pointer, even though the LogicalOperator already contains a pointer for type erasure, due to how the types reference each other
    /// But, I think we can get rid of this unnecessary level of indirection when we replace the inheritance of Operators with Concepts
    std::unique_ptr<LogicalOperator> producedBy;
    Identifier name;
    DataType dataType{};
};
}

FMT_OSTREAM(NES::Field);
