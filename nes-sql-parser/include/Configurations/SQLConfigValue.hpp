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
#include "Configurations/ConfigField.hpp"
#include "Configurations/ConfigValue.hpp"
#include "Identifiers/QualifiedIdentifier.hpp"

namespace NES {

class SQLConfigValue {
    QualifiedIdentifier name;
    ConfigLiteral value;
public:
    SQLConfigValue(QualifiedIdentifier name, ConfigLiteral value) : name(std::move(name)), value(std::move(value)) { }
    [[nodiscard]] QualifiedIdentifier getFullyQualifiedName() const { return name; }
    [[nodiscard]] ConfigLiteral getValue() const { return value; }
};

struct InvalidSQLConfigSpecification {
    std::vector<QualifiedIdentifier> unresolvableFields;
    std::vector<std::pair<QualifiedIdentifier, Exception>> failedInstantiations;
    std::vector<QualifiedIdentifier> missingFields;

    friend std::ostream& operator<<(std::ostream& os, const InvalidSQLConfigSpecification&);
};

std::expected<Schema<ConfigValue, Ordered>, InvalidSQLConfigSpecification> resolveConfig(Schema<SQLConfigValue, Ordered> passedConfig, Schema<QualifiedErasedConfigField, Ordered> declaredConfig);

}
