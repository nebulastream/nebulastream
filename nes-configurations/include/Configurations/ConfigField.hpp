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
#include <cstdint>

#include "Identifiers/QualifiedIdentifier.hpp"

namespace NES {
using ConfigLiteral = std::variant<std::string, int64_t, uint64_t, double, bool, Schema<>>;

template <typename T>
class ConfigField {
    QualifiedIdentifier name;
    std::type_index type;

public:
    ConfigField(QualifiedIdentifier name, )
    [[nodiscard]] QualifiedIdentifier getFullyQualifiedName() const { return name; }
};

}