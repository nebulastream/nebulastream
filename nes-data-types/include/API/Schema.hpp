//
// Created by ls on 4/19/25.
//

#pragma once
#include <string>
#include <fmt/core.h>
#include <fmt/ranges.h>
#include <DataType.hpp>

namespace NES
{
class Schema
{
public:
    struct Identifier
    {
        std::string name;
        std::optional<std::string> table;

        bool operator==(const Identifier&) const = default;
        struct hash
        {
            size_t operator()(const Identifier& identifier) const noexcept
            {
                return std::hash<std::string>{}(identifier.name) ^ std::hash<std::string>{}(identifier.table.value_or(""));
            }
        };
    };

private:
    std::unordered_map<Identifier, DataType, Identifier::hash> fields;

public:
    const std::unordered_map<Identifier, DataType, Identifier::hash>& getFields() const;
    std::optional<std::pair<Identifier, DataType>> getFieldByName(std::string name) const;
    std::optional<std::pair<Identifier, DataType>> getFieldByName(const Identifier& fieldName) const;

    bool addField(Identifier fieldName, DataType type);
    bool operator==(const Schema&) const = default;

    friend std::string format_as(const Schema&);
};
std::string format_as(const Schema::Identifier& identifier);
std::string format_as(const Schema& schema);
}


template <>
struct std::hash<NES::Schema::Identifier> : NES::Schema::Identifier::hash
{
};
