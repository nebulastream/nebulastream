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
#include <any>
#include <boost/core/demangle.hpp>
#include "Identifiers/QualifiedIdentifier.hpp"

namespace NES
{

class ConfigValue
{
    QualifiedIdentifier name;
    std::any value;

public:
    ConfigValue(QualifiedIdentifier name, std::any value) : name(std::move(name)), value(std::move(value)) {
        PRECONDITION(value.has_value(), "Cannot create a ConfigValue with an empty value");
    }

    [[nodiscard]] QualifiedIdentifier getFullyQualifiedName() const { return name; }

    template <typename T>
    [[nodiscard]] T getValue() const
    {
        PRECONDITION(typeid(T) == value.type(), "Stored config type {} does not match requested type {}", boost::core::demangle(value.type().name()), boost::core::demangle(typeid(T).name()));
        return value;
    }
};
}