//
// Created by ls on 4/22/25.
//
#include <string>
#include <API/Schema.hpp>

namespace NES
{
std::string format_as(const Schema::Identifier& identifier)
{
    if (identifier.table)
    {
        return fmt::format("{}.{}", *identifier.table, identifier.name);
    }
    return identifier.name;
}
std::string format_as(const Schema& schema)
{
    return fmt::format("{}", fmt::join(schema.fields, ", "));
}

const std::unordered_map<Schema::Identifier, DataType, Schema::Identifier::hash>& Schema::getFields() const
{
    return fields;
}
std::optional<std::pair<Schema::Identifier, DataType>> Schema::getFieldByName(std::string name) const
{
    return getFieldByName(Identifier{std::move(name), {}});
}
std::optional<std::pair<Schema::Identifier, DataType>> Schema::getFieldByName(const Identifier& fieldName) const
{
    if (!fieldName.table)
    {
        auto field = std::ranges::find_if(fields, [&fieldName](const auto& field) { return field.first.name == fieldName.name; });
        if (field != fields.end())
        {
            return std::make_pair(field->first, field->second);
        }
        return {};
    }

    if (auto it = fields.find(fieldName); it != fields.end())
    {
        return *it;
    }
    return {};
}
bool Schema::addField(Identifier fieldName, DataType type)
{
    return fields.emplace(std::move(fieldName), std::move(type)).second;
}
}