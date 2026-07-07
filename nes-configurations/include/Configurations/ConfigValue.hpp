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
#include <utility>
#include <vector>
#include <Schema/Schema.hpp>
#include <boost/core/demangle.hpp>
#include "Identifiers/QualifiedIdentifier.hpp"

#include "ConfigField.hpp"

namespace NES
{

/// A single instantiated configuration value: the declared field's name plus the type-erased value
/// its factory produced. Deliberately not serializable — serialization of catalog objects goes
/// through the source-defined config struct (see the SourceConfig registry), not the generic config.
class ConfigValue
{
    QualifiedIdentifier name;
    std::any value;

public:
    ConfigValue(QualifiedIdentifier name, std::any value) : name(std::move(name)), value(std::move(value))
    {
        PRECONDITION(this->value.has_value(), "Cannot create a ConfigValue with an empty value");
    }

    [[nodiscard]] QualifiedIdentifier getFullyQualifiedName() const { return name; }

    template <typename T>
    [[nodiscard]] T getValue() const
    {
        PRECONDITION(
            typeid(T) == value.type(),
            "Stored config type {} does not match requested type {}",
            boost::core::demangle(value.type().name()),
            boost::core::demangle(typeid(T).name()));
        return std::any_cast<T>(value);
    }

    template <typename T>
    [[nodiscard]] std::optional<T> tryGetValue() const
    {
        if (const auto* typed = std::any_cast<T>(&value))
        {
            return *typed;
        }
        return std::nullopt;
    }

    friend std::ostream& operator<<(std::ostream& os, const ConfigValue& value) { return os << value.getFullyQualifiedName(); }
};

class InstantiatedConfig
{
    Schema<ConfigValue, Ordered> values;

public:
    explicit InstantiatedConfig(Schema<ConfigValue, Ordered> values) : values(std::move(values)) { }

    template <typename T>
    T get(const ConfigField<T>& field) const
    {
        /// Field names are unqualified; the schema resolves them against any unambiguous suffix
        /// of the stored fully qualified names (e.g. SEED matches GENERATOR_SOURCE.SEED).
        auto valueOpt = values.getFieldByName(QualifiedIdentifier{std::vector{field.getName()}});
        PRECONDITION(valueOpt.has_value(), "Could not find config value for field {}", field.getName());
        return valueOpt.value().template getValue<T>();
    }

    /// Non-throwing lookup by name, for callers without the declaring ConfigField at hand
    /// (e.g. tooling that inspects configs). Returns nullopt if the field is absent or the
    /// stored type differs.
    template <typename T>
    [[nodiscard]] std::optional<T> tryGet(const Identifier& name) const
    {
        if (auto valueOpt = values.getFieldByName(QualifiedIdentifier{std::vector{name}}))
        {
            return valueOpt->template tryGetValue<T>();
        }
        return std::nullopt;
    }
};
}

FMT_OSTREAM(NES::ConfigValue);