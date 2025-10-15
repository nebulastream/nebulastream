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
#include <fmt/ranges.h>

namespace NES
{
struct UnboundField
{
    UnboundField(IdentifierList name, DataType dataType) : name(std::move(name)), dataType(std::move(dataType)) { }

    UnboundField(IdentifierList name, DataType::Type dataType)
        : name(std::move(name)), dataType(DataTypeProvider::provideDataType(dataType))
    {
    }

    [[nodiscard]] const IdentifierList& getName() const { return name; }

    [[nodiscard]] const DataType& getDataType() const { return dataType; }

    friend bool operator==(const UnboundField& lhs, const UnboundField& rhs);
    friend std::ostream& operator<<(std::ostream& os, const UnboundField& obj);

private:
    IdentifierList name;
    DataType dataType;
};
}

FMT_OSTREAM(NES::UnboundField);

namespace NES
{

class UnboundSchema
{
public:
    // explicit UnboundSchemaBase(std::initializer_list<UnboundFieldBase<MaxIdentifierSize>> fields);
    explicit UnboundSchema(std::vector<UnboundField> fields) : fields(std::move(fields))
    {
        using IdSpan = std::span<const Identifier>;
        std::unordered_map<IdSpan, size_t, std::hash<IdSpan>, IdentifierList::SpanEquals> fieldsByName{};
        std::unordered_map<IdSpan, std::vector<size_t>, std::hash<IdSpan>, IdentifierList::SpanEquals> collisions{};
        for (const auto& [idxSigned, field] : this->fields | std::views::enumerate)
        {
            sizeInBytes += field.getDataType().getSizeInBytes();

            INVARIANT(idxSigned >= 0, "negative index");
            const size_t idx = static_cast<size_t>(idxSigned);
            const auto& fullName = field.getName();
            for (size_t i = 0; i < std::ranges::size(fullName); i++)
            {
                IdSpan idSubSpan = std::span{std::ranges::begin(fullName) + i, std::ranges::size(fullName) - i};
                if (auto existingCollisions = collisions.find(idSubSpan); existingCollisions == collisions.end())
                {
                    if (auto existingIdList = fieldsByName.find(idSubSpan); existingIdList != fieldsByName.end())
                    {
                        collisions.insert(std::pair{idSubSpan, std::vector{idx}});
                        fieldsByName.erase(existingIdList);
                    }
                    else
                    {
                        fieldsByName.insert(std::pair{idSubSpan, idx});
                    }
                }
                else
                {
                    existingCollisions->second.push_back(idx);
                }
            }
        }
        if (!collisions.empty())
        {
            NES_DEBUG("Duplicate identifiers in schema: {}", fmt::join(collisions, ", "));
        }
        this->fieldsByName = fieldsByName
            | std::views::transform([](const auto& pair) { return std::pair{IdentifierList{pair.first}, pair.second}; })
            | std::ranges::to<std::unordered_map>();
    }

    explicit UnboundSchema(const std::initializer_list<UnboundField> fields) : UnboundSchema(std::vector(fields)) { }

    UnboundSchema() = default;

    friend bool operator==(const UnboundSchema& lhs, const UnboundSchema& rhs) { return lhs.fields == rhs.fields; }

    friend std::ostream& operator<<(std::ostream& os, const UnboundSchema& obj)
    {
        return os << fmt::format("UnboundSchema: (fields: ({})", fmt::join(obj.fields, ", "));
    }

    std::optional<UnboundField> getFieldByName(const IdentifierList& name) const
    {
        auto iter = fieldsByName.find(name);
        if (iter == fieldsByName.end())
        {
            return std::nullopt;
        }
        return fields.at(iter->second);
    };

    [[nodiscard]] UnboundField operator[](const size_t index) const { return fields.at(index); }

    size_t getSizeInBytes() const { return sizeInBytes; }

    [[nodiscard]] auto begin() const -> decltype(std::declval<std::vector<UnboundField>>().cbegin()) { return fields.cbegin(); }

    [[nodiscard]] auto end() const -> decltype(std::declval<std::vector<UnboundField>>().cend()) { return fields.cend(); }

private:
    std::vector<UnboundField> fields;
    std::unordered_map<IdentifierList, size_t> fieldsByName;
    size_t sizeInBytes = 0;
};


}

template <>
struct std::hash<NES::UnboundField>
{
    size_t operator()(const NES::UnboundField& field) const noexcept
    {
        return std::hash<NES::IdentifierList>{}(field.getName()) ^ std::hash<NES::DataType>{}(field.getDataType());
    }
};

FMT_OSTREAM(NES::UnboundSchema);