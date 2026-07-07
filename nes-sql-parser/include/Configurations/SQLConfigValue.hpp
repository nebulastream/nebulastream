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

#include <expected>
#include <utility>

#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigResolution.hpp>
#include <Configurations/ConfigValue.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>

namespace NES
{

/// A single (possibly qualified) config assignment as parsed from SQL, e.g.
/// `'ALL' AS "SOURCE".STOP_GENERATOR_WHEN_SEQUENCE_FINISHES`. The literal is typed by the parser.
class SQLConfigValue
{
    QualifiedIdentifier name;
    ConfigLiteral value;

public:
    SQLConfigValue(QualifiedIdentifier name, ConfigLiteral value) : name(std::move(name)), value(std::move(value)) { }
    [[nodiscard]] QualifiedIdentifier getFullyQualifiedName() const { return name; }
    [[nodiscard]] ConfigLiteral getValue() const { return value; }
};

/// SQL frontend adapter over the generic config resolution (Configurations/ConfigResolution.hpp).
using InvalidSQLConfigSpecification = InvalidConfigSpecification;

std::expected<Schema<ConfigValue, Ordered>, InvalidSQLConfigSpecification>
resolveConfig(const Schema<SQLConfigValue, Ordered>& passedConfig, const Schema<QualifiedErasedConfigField, Ordered>& declaredConfig);

}
