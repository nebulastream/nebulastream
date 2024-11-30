//
// Created by ls on 11/28/24.
//

#include <API/Schema.hpp>

namespace NES
{

std::optional<Field> Schema::find(const FieldName& fieldName) const
{
    if (auto it = fields.find(fieldName); it != fields.end())
    {
        return it->second;
    }
    return std::nullopt;
}
const Field& Schema::operator[](const FieldName& fieldName) const
{
    return fields.at(fieldName);
}
bool Schema::try_add(FieldName name, Field field)
{
    return fields.try_emplace(std::move(name), std::move(field)).second;
}
bool Schema::try_add(FieldName name, DataType dataType)
{
    return fields.try_emplace(std::move(name), dataType, "", Attributes{}).second;
}
bool Schema::override(FieldName name, Field field)
{
    return fields.insert_or_assign(std::move(name), field).second;
}
bool Schema::override(FieldName name, DataType dataType)
{
    return fields.insert_or_assign(std::move(name), Field{dataType, "", Attributes{}}).second;
}

std::ostream& operator<<(std::ostream& os, const Schema& obj)
{
    if (obj.fields.empty())
    {
        return os;
    }

    auto formatField = [&](const FieldName& fieldName, const Field& field, std::ostream& os)
    {
        if (!field.origin.empty())
        {
            os << field.origin << ".";
        }
        os << fieldName;
        os << ": ";
        os << field.type;
        os << "(" << field.attributes << ")";
    };

    const auto& [name, field] = *(obj.fields.begin());
    formatField(name, field, os);
    for (const auto& [name, field] : obj.fields | std::views::drop(1))
    {
        os << ", ";
        formatField(name, field, os);
    }
    return os;
}
}