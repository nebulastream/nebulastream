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
#include <ostream>
#include <ranges>
#include <unordered_map>
#include <utility>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Schema/Schema.hpp>
#include <Util/Logger/Formatter.hpp>
#include <ErrorHandling.hpp>

#include <Configurations/ConfigField.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Any.hpp>

namespace NES
{

/// A single instantiated configuration value: the declared field's name plus the type-erased value
/// its factory produced.
class InstantiatedConfigValue
{
    QualifiedIdentifier name;
    ConfigFieldId originalFieldAddress;
    ExplicitAny value;

public:
    InstantiatedConfigValue(QualifiedIdentifier name, const ConfigFieldId originalFieldAddress, ExplicitAny value)
        : name(std::move(name)), originalFieldAddress(originalFieldAddress), value(std::move(value))
    {
        PRECONDITION(this->value.hasValue(), "Cannot create a ConfigValue with an empty value");
    }

    [[nodiscard]] QualifiedIdentifier getFullyQualifiedName() const { return name; }

    template <typename T>
    [[nodiscard]] T getValue() const
    {
        return value.getAs<T>();
    }

    [[nodiscard]] const ExplicitAny& getErasedValue() const { return value; }

    [[nodiscard]] ConfigFieldId getOriginalFieldAddress() const { return originalFieldAddress; }

    friend std::ostream& operator<<(std::ostream& os, const InstantiatedConfigValue& value) { return os << value.getFullyQualifiedName(); }
};

/// A wrapper around validated and typed config values, providing a type safe access via the ConfigFields used to validate/create the values.
class InstantiatedConfig
{
    /// It is fully intended that a ConfigField under two different prefixes collapses back into one entry in this map.
    /// Whoever depends on the declared fields should not need to know anything about the prefixes of the configs where there are used at
    std::unordered_map<ConfigFieldId, ExplicitAny> values;

public:
    explicit InstantiatedConfig(const Schema<InstantiatedConfigValue, Ordered>& values)
        : values(
              values
              | std::views::transform([](const InstantiatedConfigValue& configValue)
                                      { return std::pair{configValue.getOriginalFieldAddress(), configValue.getErasedValue()}; })
              | std::ranges::to<std::unordered_map>())
    {
    }

    template <typename T>
    T get(const ConfigField<T>& field) const
    {
        const auto valueIter = values.find(ConfigFieldId{field});
        PRECONDITION(
            valueIter != values.end(), "Could not find value for config field {} at address {}", field.getName(), ConfigFieldId{field});
        return valueIter->second.getAs<T>();
    }
};
}

FMT_OSTREAM(NES::InstantiatedConfigValue);
