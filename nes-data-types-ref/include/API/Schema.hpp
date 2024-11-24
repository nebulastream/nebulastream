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

namespace NES
{

class Schema
{
public:
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

    struct Field
    {
        DataType type;
        std::string origin{};
        Attributes attributes{};
        bool operator==(const Field& rhs) const = default;
    };

    const Field& operator[](const std::string& fieldName) const { return fields.at(fieldName); }
    void add(std::string name, Field field) { fields.try_emplace(std::move(name), std::move(field)); }
    void add(std::string name, DataType dataType) { fields.try_emplace(std::move(name), dataType, "", Attributes{}); }
    void erase(const std::string& fieldName) { fields.erase(fieldName); }
    bool operator==(const Schema&) const = default;
    friend std::ostream& operator<<(std::ostream& os, const Schema& obj)
    {
        if (obj.fields.empty()) {
            return os;
}

        auto formatField = [&](const std::string& fieldName, const Field& field, std::ostream& os)
        {
            if (!field.origin.empty())
            {
                os << field.origin << ".";
            }
            os << fieldName;
            os << ": ";
            os << field.type;
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
    auto begin() { return std::begin(fields); }
    auto end() { return std::end(fields); }

private:
    std::unordered_map<std::string, Field> fields;
};

}

FMT_OSTREAM(NES::Schema);
