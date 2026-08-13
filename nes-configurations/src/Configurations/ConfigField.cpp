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

#include <Configurations/ConfigField.hpp>

#include <any>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <ostream>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <Configurations/ConfigLiteral.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Any.hpp>
#include <Util/Overloaded.hpp>
#include <fmt/base.h>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES
{

std::ostream& operator<<(std::ostream& os, const ConfigFieldId& fieldAddress)
{
    return os << fmt::format("{:#x}", fieldAddress.fieldAddress);
}

/// Not `= default`: an out-of-line defaulted friend comparison crashes clang 19 (fixed in newer clang)
bool operator==(const ConfigFieldId& lhs, const ConfigFieldId& rhs)
{
    return lhs.fieldAddress == rhs.fieldAddress;
}

std::ostream& operator<<(std::ostream& os, const QualifiedErasedConfigField& field)
{
    return os << field.name;
}

bool operator==(const ConfigFieldDefault& lhs, const ConfigFieldDefault& rhs)
{
    return lhs.name == rhs.name;
}

std::ostream& operator<<(std::ostream& os, const ConfigFieldDefault& obj)
{
    return os << fmt::format("ConfigFieldDefault({})", obj.name);
}

bool operator==(const ConfigFieldTransformation& lhs, const ConfigFieldTransformation& rhs)
{
    return lhs.name == rhs.name;
}

std::ostream& operator<<(std::ostream& os, const ConfigFieldTransformation& obj)
{
    return os << fmt::format("ConfigFieldTransformation({})", obj.name);
}
}

std::expected<double, NES::Exception> NES::tryGetDoubleOrInt(const ConfigLiteral& literal, const std::function<Exception()>& orElse)
{
    if (const auto* asDouble = std::get_if<double>(&literal))
    {
        return *asDouble;
    }
    if (const auto* asInteger = std::get_if<int64_t>(&literal))
    {
        return static_cast<double>(*asInteger);
    }
    return std::unexpected{orElse()};
}

NES::QualifiedErasedConfigField::QualifiedErasedConfigField(
    QualifiedIdentifier name,
    ConfigFieldId originalFieldAddress,
    std::function<std::expected<ExplicitAny, Exception>(const ConfigLiteral&)> factory,
    std::optional<std::function<std::any()>> defaultSupplier,
    std::string description,
    std::optional<std::string> defaultDescription)
    : name(std::move(name))
    , originalFieldAddress(originalFieldAddress)
    , factory(std::move(factory))
    , defaultSupplier(std::move(defaultSupplier))
    , description(std::move(description))
    , defaultDescription(std::move(defaultDescription))
{
}

const NES::QualifiedIdentifier& NES::QualifiedErasedConfigField::getFullyQualifiedName() const
{
    return name;
}

std::string_view NES::QualifiedErasedConfigField::getDescription() const
{
    return description;
}

const std::optional<std::string>& NES::QualifiedErasedConfigField::getDefaultDescription() const
{
    return defaultDescription;
}

std::expected<NES::ExplicitAny, NES::Exception> NES::QualifiedErasedConfigField::apply(const ConfigLiteral& literal) const
{
    return factory(literal);
}

const std::function<std::expected<NES::ExplicitAny, NES::Exception>(const NES::ConfigLiteral&)>&
NES::QualifiedErasedConfigField::getFactory() const
{
    return factory;
}

bool NES::QualifiedErasedConfigField::hasDefault() const
{
    return defaultSupplier.has_value();
}

std::any NES::QualifiedErasedConfigField::getDefault() const
{
    PRECONDITION(defaultSupplier.has_value(), "getDefault requires a declared default, check hasDefault() first");
    return defaultSupplier.value()();
}

const std::optional<std::function<std::any()>>& NES::QualifiedErasedConfigField::getDefaultSupplier() const
{
    return defaultSupplier;
}

const NES::ConfigFieldId& NES::QualifiedErasedConfigField::getOriginalFieldAddress() const
{
    return originalFieldAddress;
}

NES::ConfigFieldDefault::ConfigFieldDefault(QualifiedIdentifier name, std::function<ConfigLiteral()> supplier)
    : name(std::move(name)), supplier(std::move(supplier))
{
}

NES::ConfigFieldDefault::ConfigFieldDefault(QualifiedIdentifier name, ConfigLiteral value)
    : name(std::move(name)), supplier([value = std::move(value)]() { return value; })
{
}

NES::ConfigLiteral NES::ConfigFieldDefault::get() const
{
    return supplier();
}

const NES::QualifiedIdentifier& NES::ConfigFieldDefault::getFullyQualifiedName() const
{
    return name;
}

const NES::QualifiedIdentifier& NES::ConfigFieldTransformation::getFullyQualifiedName() const
{
    return name;
}

const std::function<std::expected<NES::ExplicitAny, NES::Exception>(const NES::ExplicitAny&)>&
NES::ConfigFieldTransformation::getTransformation() const
{
    return transformation;
}

std::size_t std::hash<NES::ConfigFieldDefault>::operator()(const NES::ConfigFieldDefault& obj) const noexcept
{
    return std::hash<NES::QualifiedIdentifier>{}(obj.getFullyQualifiedName());
}

std::size_t std::hash<NES::ConfigFieldTransformation>::operator()(const NES::ConfigFieldTransformation& obj) const noexcept
{
    return std::hash<NES::QualifiedIdentifier>{}(obj.name);
}

std::size_t std::hash<NES::ConfigFieldId>::operator()(const NES::ConfigFieldId& obj) const noexcept
{
    return obj.fieldAddress;
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
fmt::context::iterator fmt::formatter<NES::ConfigLiteral>::format(const NES::ConfigLiteral& literal, format_context& ctx) const
{
    return fmt::formatter<std::string_view>{}.format(
        std::visit(
            NES::Overloaded{
                [](const std::monostate&) { return std::string{"null"}; },
                [](const NES::Schema<NES::UnqualifiedUnboundField, NES::Ordered>& schema)
                {
                    std::ostringstream oss;
                    oss << schema;
                    return std::move(oss).str();
                },
                [](const auto& value) { return fmt::format("{}", value); }},
            literal),
        ctx);
}
