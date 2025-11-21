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
#include "ErrorHandling.hpp"


#include <functional>
#include <ostream>
#include <span>
#include <unordered_map>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Identifiers/Identifier.hpp>
#include <Util/Logger/Formatter.hpp>
#include <fmt/ranges.h>

namespace NES
{
template <size_t IdListExtent>
struct UnboundFieldBase
{
    UnboundFieldBase(IdentifierListBase<IdListExtent> name, DataType dataType) : name(std::move(name)), dataType(std::move(dataType)) { }

    UnboundFieldBase(IdentifierListBase<IdListExtent> name, DataType::Type dataType)
        : name(std::move(name)), dataType(DataTypeProvider::provideDataType(dataType))
    {
    }

    template <size_t OtherIdListExtent>
    UnboundFieldBase(const UnboundFieldBase<OtherIdListExtent>& other)
    requires(IdListExtent == std::dynamic_extent || IdListExtent != OtherIdListExtent)
        : name(other.getName()), dataType(other.getDataType())
    {
    }

    [[nodiscard]] const IdentifierListBase<IdListExtent>& getName() const { return name; }

    [[nodiscard]] const DataType& getDataType() const { return dataType; }

    friend bool operator==(const UnboundFieldBase& lhs, const UnboundFieldBase& rhs)
    {
        return lhs.getName() == rhs.getName() && lhs.getDataType() == rhs.getDataType();
    }

    friend std::ostream& operator<<(std::ostream& os, const UnboundFieldBase& obj)
    {
        return os << fmt::format("UnboundField: (name: {}, type: {})", obj.getName(), obj.getDataType());
    }

private:
    IdentifierListBase<IdListExtent> name;
    DataType dataType;
};

using UnboundField = UnboundFieldBase<std::dynamic_extent>;
}

template <size_t IdListExtent>
struct fmt::formatter<NES::UnboundFieldBase<IdListExtent>> : fmt::ostream_formatter
{
};

namespace NES
{


template <size_t IdListExtent>
class UnboundSchemaBase
{
public:
    // explicit UnboundSchemaBase(std::initializer_list<UnboundFieldBase<MaxIdentifierSize>> fields);
    explicit UnboundSchemaBase(std::vector<UnboundFieldBase<IdListExtent>> fields) : fields(std::move(fields))
    {
        using IdSpan = std::span<const Identifier, IdListExtent>;
        std::unordered_map<IdSpan, size_t, std::hash<IdSpan>, IdentifierList::SpanEquals<IdListExtent>> fieldsByName{};
        std::unordered_map<IdSpan, std::vector<size_t>, std::hash<IdSpan>, IdentifierList::SpanEquals<IdListExtent>> collisions{};
        for (const auto& [idxSigned, field] : this->fields | std::views::enumerate)
        {
            sizeInBytes += field.getDataType().getSizeInBytes();
            INVARIANT(idxSigned >= 0, "negative index");
            const auto idx = static_cast<size_t>(idxSigned);
            const auto& fullName = field.getName();
            for (size_t i = 0; i < std::ranges::size(fullName); i++)
            {
                IdSpan idSubSpan = IdSpan{std::ranges::begin(fullName) + i, std::ranges::size(fullName) - i};
                if (auto existingCollisions = collisions.find(idSubSpan); existingCollisions == collisions.end())
                {
                    if (auto existingIdList = fieldsByName.find(idSubSpan); existingIdList != fieldsByName.end())
                    {
                        collisions.insert(std::pair{idSubSpan, std::vector{existingIdList->second, idx}});
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
            | std::views::transform([](const auto& pair) { return std::pair{IdentifierListBase<IdListExtent>{pair.first}, pair.second}; })
            | std::ranges::to<std::unordered_map>();
    }

    explicit UnboundSchemaBase(const std::initializer_list<UnboundFieldBase<IdListExtent>> fields) : UnboundSchemaBase{std::vector(fields)}
    {
    }

    UnboundSchemaBase() = default;

    template <size_t RhsExtent>
    friend bool operator==(const UnboundSchemaBase& lhs, const UnboundSchemaBase<RhsExtent>& rhs)
    {
        return lhs.fields == rhs.fields;
    }

    friend std::ostream& operator<<(std::ostream& os, const UnboundSchemaBase& obj)
    {
        return os << fmt::format("UnboundSchema: (fields: ({})", fmt::join(obj.fields, ", "));
    }

    std::optional<UnboundField> operator[](const IdentifierList& name) const
    {
        auto iter = fieldsByName.find(name);
        if (iter == fieldsByName.end())
        {
            return std::nullopt;
        }
        return fields.at(iter->second);
    };

    [[nodiscard]] std::optional<UnboundField> operator[](const size_t index) const
    {
        if (index < std::ranges::size(fields))
        {
            return fields.at(index);
        }
        return std::nullopt;
    }

    size_t getSizeInBytes() const { return sizeInBytes; }

    [[nodiscard]] auto begin() const -> decltype(std::declval<std::vector<UnboundFieldBase<IdListExtent>>>().cbegin())
    {
        return fields.cbegin();
    }

    [[nodiscard]] auto end() const -> decltype(std::declval<std::vector<UnboundFieldBase<IdListExtent>>>().cend()) { return fields.cend(); }

private:
    std::vector<UnboundFieldBase<IdListExtent>> fields;
    std::unordered_map<IdentifierListBase<IdListExtent>, size_t> fieldsByName;
    size_t sizeInBytes = 0;
};

using UnboundSchema = UnboundSchemaBase<std::dynamic_extent>;

}

template <size_t IdListExtent>
struct std::hash<NES::UnboundFieldBase<IdListExtent>>
{
    size_t operator()(const NES::UnboundFieldBase<IdListExtent>& field) const noexcept
    {
        return std::hash<NES::IdentifierListBase<IdListExtent>>{}(field.getName()) ^ std::hash<NES::DataType>{}(field.getDataType());
    }
};

FMT_OSTREAM(NES::UnboundSchema);