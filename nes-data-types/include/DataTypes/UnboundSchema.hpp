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
#include "DataTypeProvider.hpp"


#include <ostream>
#include <unordered_map>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Identifiers/Identifier.hpp>
#include <Util/Logger/Formatter.hpp>

namespace NES
{

struct UnboundField
{
    UnboundField(Identifier name, DataType dataType) : name(std::move(name)), dataType(std::move(dataType)) {}
    UnboundField(Identifier name, DataType::Type dataType) : name(std::move(name)), dataType(DataTypeProvider::provideDataType(dataType)) {}

    [[nodiscard]] const Identifier& getName() const { return name; }
    [[nodiscard]] const DataType& getDataType() const { return dataType; }
    
    friend bool operator==(const UnboundField& lhs, const UnboundField& rhs);
    friend std::ostream& operator<<(std::ostream& os, const UnboundField& obj);
private:
    Identifier name;
    DataType dataType;
};

class UnboundSchema
{
public:
    explicit UnboundSchema(std::initializer_list<UnboundField> fields);
    explicit UnboundSchema(std::vector<UnboundField> fields);
    UnboundSchema() = default;

    friend bool operator==(const UnboundSchema& lhs, const UnboundSchema& rhs);
    friend std::ostream& operator<<(std::ostream& os, const UnboundSchema& obj);

    std::optional<UnboundField> getFieldByName(const Identifier& name) const;
    size_t getSizeInBytes() const;

    [[nodiscard]] auto begin() const -> decltype(std::declval<std::vector<UnboundField>>().cbegin());

    [[nodiscard]] auto end() const -> decltype(std::declval<std::vector<UnboundField>>().cend());

private:
    std::vector<UnboundField> fields;
    std::unordered_map<Identifier, size_t> fieldsByName;
    size_t sizeInBytes = 0;
};
}

template<>
struct std::hash<NES::UnboundField>
{
    size_t operator()(const NES::UnboundField& field) const noexcept
    {
        return std::hash<NES::Identifier>{}(field.getName()) ^ std::hash<NES::DataType>{}(field.getDataType());
    }
};

FMT_OSTREAM(NES::UnboundField);
FMT_OSTREAM(NES::UnboundSchema);