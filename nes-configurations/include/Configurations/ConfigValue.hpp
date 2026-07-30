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
#include <ostream>
#include <utility>
#include <vector>
#include <Schema/Schema.hpp>
#include <Util/Logger/Formatter.hpp>
#include <boost/core/demangle.hpp>
#include <ErrorHandling.hpp>
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
    ConfigFieldAddress originalFieldAddress;
    ExplicitAny value;

public:
    ConfigValue(QualifiedIdentifier name, const ConfigFieldAddress originalFieldAddress, ExplicitAny value)
        : name(std::move(name)), originalFieldAddress(originalFieldAddress), value(std::move(value))
    {
        PRECONDITION(this->value.getValue().has_value(), "Cannot create a ConfigValue with an empty value");
    }

    [[nodiscard]] QualifiedIdentifier getFullyQualifiedName() const { return name; }

    template <typename T>
    [[nodiscard]] T getValue() const
    {
        return value.getAs<T>();
    }

    [[nodiscard]] const ExplicitAny& getRawValue() const { return value; }

    [[nodiscard]] const ConfigFieldAddress getOriginalFieldAddress() const { return originalFieldAddress; }

    friend std::ostream& operator<<(std::ostream& os, const ConfigValue& value) { return os << value.getFullyQualifiedName(); }
};

class InstantiatedConfig
{
    std::unordered_map<ConfigFieldAddress, std::any> values;

public:
    explicit InstantiatedConfig(Schema<ConfigValue, Ordered> values)
        : values(
              values
              | std::views::transform([](const ConfigValue& configValue)
                                      { return std::pair{configValue.getOriginalFieldAddress(), configValue.getRawValue().getValue()}; })
              | std::ranges::to<std::unordered_map>())
    {
    }

    template <typename T>
    T get(const ConfigField<T>& field) const
    {
        const auto valueIter = values.find(ConfigFieldAddress{field});
        PRECONDITION(
            valueIter != values.end(),
            "Could not find value for config field {} at address {}",
            field.getName(),
            ConfigFieldAddress{field});

        const auto expectedTypeName = boost::core::demangle(typeid(T).name());
        const auto actualTypeName = boost::core::demangle(valueIter->second.type().name());
        PRECONDITION(typeid(T) == valueIter->second.type(), "Stored config type {} does not match requested type {}", actualTypeName, expectedTypeName);
        return std::any_cast<T>(valueIter->second);

    }
};
}

FMT_OSTREAM(NES::ConfigValue);