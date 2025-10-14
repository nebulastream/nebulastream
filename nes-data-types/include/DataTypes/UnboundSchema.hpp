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

template <size_t MaxIdentifierSize>
struct UnboundFieldBase
{
    UnboundFieldBase(IdentifierListBase<MaxIdentifierSize, true> name, DataType dataType)
        : name(std::move(name)), dataType(std::move(dataType))
    {
    }

    UnboundFieldBase(IdentifierListBase<MaxIdentifierSize, true> name, DataType::Type dataType)
        : name(std::move(name)), dataType(DataTypeProvider::provideDataType(dataType))
    {
    }

    [[nodiscard]] const IdentifierListBase<MaxIdentifierSize, true>& getName() const { return name; }

    [[nodiscard]] const DataType& getDataType() const { return dataType; }

    friend bool operator==(const UnboundFieldBase& lhs, const UnboundFieldBase& rhs);
    friend std::ostream& operator<<(std::ostream& os, const UnboundFieldBase& obj);

private:
    IdentifierListBase<MaxIdentifierSize, true> name;
    DataType dataType;
};

using UnboundField = UnboundFieldBase<std::dynamic_extent>;

template <size_t MaxIdentifierSize>
class UnboundSchemaBase
{
public:
    // explicit UnboundSchemaBase(std::initializer_list<UnboundFieldBase<MaxIdentifierSize>> fields);
    explicit UnboundSchemaBase(std::vector<UnboundFieldBase<MaxIdentifierSize>> fields) : fields(std::move(fields))
    {
        using IdSpan = std::span<const Identifier, MaxIdentifierSize>;
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
            | std::views::transform([](const auto& pair)
                                    { return std::pair{IdentifierListBase<MaxIdentifierSize, true>{pair.first}, pair.second}; })
            | std::ranges::to<std::unordered_map>();
    }

    UnboundSchemaBase() = default;

    friend bool operator==(const UnboundSchemaBase& lhs, const UnboundSchemaBase& rhs) { return lhs.fields == rhs.fields; }

    friend std::ostream& operator<<(std::ostream& os, const UnboundSchemaBase& obj)
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

    size_t getSizeInBytes() const { return sizeInBytes; }

    [[nodiscard]] auto begin() const -> decltype(std::declval<std::vector<UnboundField>>().cbegin())
    {
        return fields.cbegin();
    }

    [[nodiscard]] auto end() const -> decltype(std::declval<std::vector<UnboundField>>().cend())
    {
        return fields.cend();
    }

private:
    std::vector<UnboundField> fields;
    std::unordered_map<IdentifierListBase<std::dynamic_extent, true>, size_t> fieldsByName;
    size_t sizeInBytes = 0;
};

using UnboundSchema = UnboundSchemaBase<std::dynamic_extent>;

}

template <>
struct std::hash<NES::UnboundField>
{
    size_t operator()(const NES::UnboundField& field) const noexcept
    {
        return std::hash<NES::IdentifierList>{}(field.getName()) ^ std::hash<NES::DataType>{}(field.getDataType());
    }
};

FMT_OSTREAM(NES::UnboundField);
FMT_OSTREAM(NES::UnboundSchema);