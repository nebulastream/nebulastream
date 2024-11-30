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
#include <ranges>
#include <string>
#include <unordered_map>
#include <API/DataType.hpp>
#include <Util/Logger/Formatter.hpp>


namespace NES
{
struct Attributes
{
    bool nullable = false;
    bool operator==(const Attributes& rhs) const = default;
    friend std::ostream& operator<<(std::ostream& os, const Attributes& obj)
    {
        if (obj.nullable)
        {
            os << "NULLABLE";
        }
        return os;
    }
};
struct FieldName
{
    FieldName(std::string origin, std::string name) : origin(std::move(origin)), name(std::move(name)) { }
    FieldName(std::string name) : origin(""), name(std::move(name)) { }
    FieldName() = default;
    std::string origin;
    std::string name;
    bool operator==(const FieldName& rhs) const = default;
    friend std::ostream& operator<<(std::ostream& os, const FieldName& obj) { return os << obj.origin << "." << obj.name; }
};

struct Field
{
    DataType type;
    std::string origin{};
    Attributes attributes{};
    bool operator==(const Field& rhs) const = default;
};
}

template <>
struct std::hash<NES::FieldName>
{
    size_t operator()(const NES::FieldName& fieldName) const noexcept
    {
        return std::hash<std::string>()(fieldName.name) ^ std::hash<std::string>()(fieldName.origin);
    }
};

namespace NES
{

class Schema
{
public:
    std::optional<Field> find(const FieldName& fieldName) const;

    const Field& operator[](const FieldName& fieldName) const;
    bool try_add(FieldName name, Field field);
    bool try_add(FieldName name, DataType dataType);

    bool override(FieldName name, Field field);
    bool override(FieldName name, DataType dataType);

    void erase(const FieldName& fieldName) { fields.erase(fieldName); }
    bool operator==(const Schema&) const = default;
    friend std::ostream& operator<<(std::ostream& os, const Schema& obj);
    auto begin() { return std::begin(fields); }
    auto end() { return std::end(fields); }

private:
    std::unordered_map<FieldName, Field> fields;
};

}

FMT_OSTREAM(NES::FieldName);
FMT_OSTREAM(NES::Schema);
