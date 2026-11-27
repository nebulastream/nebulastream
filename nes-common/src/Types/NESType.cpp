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

#include <Types/NESType.hpp>

#include <algorithm>
#include <cstddef>
#include <expected>
#include <memory>
#include <optional>
#include <ostream>
#include <ranges>
#include <span>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include <Identifiers/QualifiedIdentifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Any.hpp>
#include <Util/Overloaded.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES
{

namespace detail
{
struct NESValueImpl
{
    NESType type;
    ExplicitAny internalRepresentation;
};
}

namespace
{
struct AnyNESTypeClass
{
    bool operator==(const AnyNESTypeClass&) const = default;
    [[nodiscard]] static bool validate(const NESType&) { return true; }
};

struct ConjunctionNESTypeClass
{
    NESTypeClass left;
    NESTypeClass right;
    bool operator==(const ConjunctionNESTypeClass&) const = default;

    [[nodiscard]] bool validate(const NESType& type) const { return left.validate(type) && right.validate(type); }
};

struct DisjunctionNESTypeClass
{
    NESTypeClass left;
    NESTypeClass right;
    bool operator==(const DisjunctionNESTypeClass&) const = default;

    [[nodiscard]] bool validate(const NESType& type) const { return left.validate(type) || right.validate(type); }
};

struct ConstantNESParametrizedTypeClass
{
    NESTypeClass constantClass;
    bool operator==(const ConstantNESParametrizedTypeClass&) const = default;

    [[nodiscard]] std::expected<NESTypeClass, Exception> instantiate(const NESTypeClassParameters& params) const
    {
        if (!params.empty())
        {
            return std::unexpected{
                TypeInferenceException("Unparameterized type class expects 0 arguments, but got {}", params.size())};
        }
        return constantClass;
    }
};

template <typename FieldT>
std::optional<QualifiedIdentifier> findDuplicateFieldName(const Schema<FieldT, Ordered>& schema)
{
    std::unordered_set<QualifiedIdentifier> seen;
    seen.reserve(schema.size());
    for (const auto& field : schema)
    {
        if (!seen.insert(field.getFullyQualifiedName()).second)
        {
            return field.getFullyQualifiedName();
        }
    }
    return std::nullopt;
}

template <typename FieldT>
std::optional<FieldT> findExactNamedField(const Schema<FieldT, Ordered>& schema, const QualifiedIdentifier& formalName)
{
    for (const auto& field : schema)
    {
        if (field.getFullyQualifiedName() == formalName)
        {
            return field;
        }
    }
    return std::nullopt;
}
}

/// --- NESType & NamedNESType ---

NESType::NESType(
    std::shared_ptr<const detail::ErasedNESType> self,
    std::shared_ptr<const detail::TypeTraitMap> typeTraits)
    : self(std::move(self)), typeTraits(std::move(typeTraits))
{
    PRECONDITION(this->self != nullptr, "NESType implementation pointer must not be null");
}

std::expected<NESValue, Exception> NESType::construct(const NESValueParameters& params) const
{
    return self->constructRaw(params)
        .transform([this](ExplicitAny&& representation) { return NESValue{*this, std::move(representation)}; });
}

std::expected<NESValue, Exception> NESType::construct() const
{
    return construct(NESValueParameters{});
}

bool NESType::operator==(const NESType& other) const
{
    if (self == other.self)
    {
        return true;
    }
    return self->equals(*other.self);
}

bool NESType::equalsValue(const ExplicitAny& lhs, const ExplicitAny& rhs) const
{
    return self->equalsValue(lhs, rhs);
}

NamedNESType::NamedNESType(QualifiedIdentifier name, NESType underlying)
    : name(std::move(name)), underlying(std::move(underlying))
{
}

const QualifiedIdentifier& NamedNESType::getFullyQualifiedName() const
{
    return name;
}

const NESType& NamedNESType::getUnderlyingType() const
{
    return underlying;
}

bool NamedNESType::operator==(const NamedNESType& other) const
{
    return name == other.name && underlying == other.underlying;
}

std::ostream& operator<<(std::ostream& os, const NamedNESType& obj)
{
    return os << fmt::format("NamedNESType({})", obj.name);
}

/// --- NESValue, NamedNESValue & NESValueParameters ---

NESValue::NESValue(NESType type, ExplicitAny internalRepresentation)
    : impl(std::make_shared<detail::NESValueImpl>(detail::NESValueImpl{
          .type = std::move(type),
          .internalRepresentation = std::move(internalRepresentation)}))
{
}

const ExplicitAny& NESValue::getInternalRepresentation() const
{
    return impl->internalRepresentation;
}

const NESType& NESValue::getType() const
{
    return impl->type;
}

bool NESValue::operator==(const NESValue& other) const
{
    if (impl == other.impl)
    {
        return true;
    }
    return impl->type == other.impl->type && impl->type.equalsValue(impl->internalRepresentation, other.impl->internalRepresentation);
}

NamedNESValue::NamedNESValue(QualifiedIdentifier name, NESValue value)
    : name(std::move(name)), value(std::move(value))
{
}

const QualifiedIdentifier& NamedNESValue::getFullyQualifiedName() const
{
    return name;
}

const NESValue& NamedNESValue::getValue() const
{
    return value;
}

bool NamedNESValue::operator==(const NamedNESValue& other) const
{
    return name == other.name && value == other.value;
}

std::ostream& operator<<(std::ostream& os, const NamedNESValue& obj)
{
    return os << fmt::format("NamedNESValue({})", obj.name);
}

NESValueParameters::NESValueParameters() : storage(std::vector<NESValue>{})
{
}

NESValueParameters::NESValueParameters(std::vector<NESValue> positionalValues) : storage(std::move(positionalValues))
{
}

NESValueParameters::NESValueParameters(Schema<NamedNESValue, Ordered> namedValues) : storage(std::move(namedValues))
{
}

NESValueParameters NESValueParameters::positional(std::vector<NESValue> values)
{
    return NESValueParameters{std::move(values)};
}

NESValueParameters NESValueParameters::named(Schema<NamedNESValue, Ordered> values)
{
    return NESValueParameters{std::move(values)};
}

bool NESValueParameters::isNamed() const noexcept
{
    return std::holds_alternative<Schema<NamedNESValue, Ordered>>(storage);
}

size_t NESValueParameters::size() const noexcept
{
    return std::visit([](const auto& container) { return container.size(); }, storage);
}

bool NESValueParameters::empty() const noexcept
{
    return size() == 0;
}

std::optional<NESValue> NESValueParameters::at(const size_t index) const
{
    return std::visit(
        Overloaded{
            [index](const std::vector<NESValue>& pos) -> std::optional<NESValue>
            {
                if (index < pos.size())
                {
                    return pos[index];
                }
                return std::nullopt;
            },
            [index](const Schema<NamedNESValue, Ordered>& namedSchema) -> std::optional<NESValue>
            {
                return namedSchema[index].transform([](const NamedNESValue& namedVal) { return namedVal.getValue(); });
            }},
        storage);
}

std::optional<NESValue> NESValueParameters::getByName(const QualifiedIdentifier& name) const
{
    if (const auto* namedSchema = std::get_if<Schema<NamedNESValue, Ordered>>(&storage))
    {
        return namedSchema->getFieldByName(name).transform([](const NamedNESValue& namedVal) { return namedVal.getValue(); });
    }
    return std::nullopt;
}

const Schema<NamedNESValue, Ordered>* NESValueParameters::tryGetNamedSchema() const noexcept
{
    return std::get_if<Schema<NamedNESValue, Ordered>>(&storage);
}

const std::vector<NESValue>* NESValueParameters::tryGetPositional() const noexcept
{
    return std::get_if<std::vector<NESValue>>(&storage);
}

/// --- NamedNESTypeParameter & NESTypeParameters ---

NamedNESTypeParameter::NamedNESTypeParameter(QualifiedIdentifier name, NESType type)
    : value(std::move(type)), name(std::move(name))
{
}

NamedNESTypeParameter::NamedNESTypeParameter(NESType type, QualifiedIdentifier name)
    : value(std::move(type)), name(std::move(name))
{
}

NamedNESTypeParameter::NamedNESTypeParameter(QualifiedIdentifier name, NESValue val)
    : value(std::move(val)), name(std::move(name))
{
}

NamedNESTypeParameter::NamedNESTypeParameter(NESValue val, QualifiedIdentifier name)
    : value(std::move(val)), name(std::move(name))
{
}

NamedNESTypeParameter::NamedNESTypeParameter(QualifiedIdentifier name, NESFunctor functor)
    : value(std::move(functor)), name(std::move(name))
{
}

NamedNESTypeParameter::NamedNESTypeParameter(NESFunctor functor, QualifiedIdentifier name)
    : value(std::move(functor)), name(std::move(name))
{
}

NamedNESTypeParameter::NamedNESTypeParameter(QualifiedIdentifier name, Element param)
    : value(std::move(param)), name(std::move(name))
{
}

const QualifiedIdentifier& NamedNESTypeParameter::getFullyQualifiedName() const
{
    return name;
}

bool NamedNESTypeParameter::isType() const noexcept
{
    return std::holds_alternative<NESType>(value);
}

bool NamedNESTypeParameter::isValue() const noexcept
{
    return std::holds_alternative<NESValue>(value);
}

bool NamedNESTypeParameter::isFunctor() const noexcept
{
    return std::holds_alternative<NESFunctor>(value);
}

const NESType* NamedNESTypeParameter::tryGetType() const noexcept
{
    return std::get_if<NESType>(&value);
}

const NESValue* NamedNESTypeParameter::tryGetValue() const noexcept
{
    return std::get_if<NESValue>(&value);
}

const NESFunctor* NamedNESTypeParameter::tryGetFunctor() const noexcept
{
    return std::get_if<NESFunctor>(&value);
}

const NamedNESTypeParameter::Element& NamedNESTypeParameter::getUnderlying() const noexcept
{
    return value;
}

bool NamedNESTypeParameter::operator==(const NamedNESTypeParameter& other) const
{
    return name == other.name && value == other.value;
}

std::ostream& operator<<(std::ostream& os, const NamedNESTypeParameter& obj)
{
    return os << fmt::format("NamedNESTypeParameter({})", obj.name);
}

NESTypeParameters::NESTypeParameters() : storage(std::vector<Element>{})
{
}

NESTypeParameters::NESTypeParameters(std::vector<Element> positionalParams) : storage(std::move(positionalParams))
{
}

NESTypeParameters::NESTypeParameters(Schema<NamedNESTypeParameter, Ordered> namedParams) : storage(std::move(namedParams))
{
}

NESTypeParameters NESTypeParameters::fromTypes(std::vector<NESType> types)
{
    std::vector<Element> elements;
    elements.reserve(types.size());
    for (auto& type : types)
    {
        elements.emplace_back(std::move(type));
    }
    return NESTypeParameters{std::move(elements)};
}

NESTypeParameters NESTypeParameters::fromValues(std::vector<NESValue> values)
{
    std::vector<Element> elements;
    elements.reserve(values.size());
    for (auto& val : values)
    {
        elements.emplace_back(std::move(val));
    }
    return NESTypeParameters{std::move(elements)};
}

NESTypeParameters NESTypeParameters::fromFunctors(std::vector<NESFunctor> functors)
{
    std::vector<Element> elements;
    elements.reserve(functors.size());
    for (auto& functor : functors)
    {
        elements.emplace_back(std::move(functor));
    }
    return NESTypeParameters{std::move(elements)};
}

NESTypeParameters NESTypeParameters::named(Schema<NamedNESTypeParameter, Ordered> params)
{
    return NESTypeParameters{std::move(params)};
}

bool NESTypeParameters::isNamed() const noexcept
{
    return std::holds_alternative<Schema<NamedNESTypeParameter, Ordered>>(storage);
}

size_t NESTypeParameters::size() const noexcept
{
    return std::visit([](const auto& container) { return container.size(); }, storage);
}

bool NESTypeParameters::empty() const noexcept
{
    return size() == 0;
}

std::optional<NESTypeParameters::Element> NESTypeParameters::at(const size_t index) const
{
    return std::visit(
        Overloaded{
            [index](const std::vector<Element>& pos) -> std::optional<Element>
            {
                if (index < pos.size())
                {
                    return pos[index];
                }
                return std::nullopt;
            },
            [index](const Schema<NamedNESTypeParameter, Ordered>& namedSchema) -> std::optional<Element>
            {
                return namedSchema[index].transform([](const NamedNESTypeParameter& param) { return param.getUnderlying(); });
            }},
        storage);
}

std::optional<NESType> NESTypeParameters::getTypeAt(const size_t index) const
{
    if (const auto elem = at(index); elem.has_value())
    {
        if (const auto* type = std::get_if<NESType>(&*elem))
        {
            return *type;
        }
    }
    return std::nullopt;
}

std::optional<NESValue> NESTypeParameters::getValueAt(const size_t index) const
{
    if (const auto elem = at(index); elem.has_value())
    {
        if (const auto* val = std::get_if<NESValue>(&*elem))
        {
            return *val;
        }
    }
    return std::nullopt;
}

std::optional<NESFunctor> NESTypeParameters::getFunctorAt(const size_t index) const
{
    if (const auto elem = at(index); elem.has_value())
    {
        if (const auto* functor = std::get_if<NESFunctor>(&*elem))
        {
            return *functor;
        }
    }
    return std::nullopt;
}

std::optional<NamedNESTypeParameter> NESTypeParameters::getByName(const QualifiedIdentifier& name) const
{
    if (const auto* namedSchema = std::get_if<Schema<NamedNESTypeParameter, Ordered>>(&storage))
    {
        return namedSchema->getFieldByName(name);
    }
    return std::nullopt;
}

std::optional<NESType> NESTypeParameters::getTypeByName(const QualifiedIdentifier& name) const
{
    if (const auto found = getByName(name); found.has_value() && found->isType())
    {
        return *found->tryGetType();
    }
    return std::nullopt;
}

std::optional<NESValue> NESTypeParameters::getValueByName(const QualifiedIdentifier& name) const
{
    if (const auto found = getByName(name); found.has_value() && found->isValue())
    {
        return *found->tryGetValue();
    }
    return std::nullopt;
}

std::optional<NESFunctor> NESTypeParameters::getFunctorByName(const QualifiedIdentifier& name) const
{
    if (const auto found = getByName(name); found.has_value() && found->isFunctor())
    {
        return *found->tryGetFunctor();
    }
    return std::nullopt;
}

const Schema<NamedNESTypeParameter, Ordered>* NESTypeParameters::tryGetNamedSchema() const noexcept
{
    return std::get_if<Schema<NamedNESTypeParameter, Ordered>>(&storage);
}

const std::vector<NESTypeParameters::Element>* NESTypeParameters::tryGetPositional() const noexcept
{
    return std::get_if<std::vector<Element>>(&storage);
}

bool NESTypeParameters::operator==(const NESTypeParameters& other) const
{
    return storage == other.storage;
}

/// --- NESTypeClass & Parameterized Type Classes ---

NESTypeClass NESTypeClass::any()
{
    return NESTypeClass{AnyNESTypeClass{}};
}

bool NESTypeClass::validate(const NESType& type) const
{
    return self->validate(type);
}

bool NESTypeClass::operator==(const NESTypeClass& other) const
{
    if (self == other.self)
    {
        return true;
    }
    return self->equals(*other.self);
}

NESTypeClass operator&&(const NESTypeClass& lhs, const NESTypeClass& rhs)
{
    return NESTypeClass{ConjunctionNESTypeClass{.left = lhs, .right = rhs}};
}

NESTypeClass operator||(const NESTypeClass& lhs, const NESTypeClass& rhs)
{
    return NESTypeClass{DisjunctionNESTypeClass{.left = lhs, .right = rhs}};
}

NamedNESTypeClass::NamedNESTypeClass(QualifiedIdentifier name, NESTypeClass typeClass)
    : name(std::move(name)), typeClass(std::move(typeClass))
{
}

const QualifiedIdentifier& NamedNESTypeClass::getFullyQualifiedName() const
{
    return name;
}

const NESTypeClass& NamedNESTypeClass::getTypeClass() const
{
    return typeClass;
}

bool NamedNESTypeClass::validate(const NESType& type) const
{
    return typeClass.validate(type);
}

bool NamedNESTypeClass::operator==(const NamedNESTypeClass& other) const
{
    return name == other.name && typeClass == other.typeClass;
}

std::ostream& operator<<(std::ostream& os, const NamedNESTypeClass& obj)
{
    return os << fmt::format("NamedNESTypeClass({})", obj.name);
}

NESTypeClassParameters::NESTypeClassParameters(NESTypeParameters parameters) : parameters(std::move(parameters))
{
}

const NESTypeParameters& NESTypeClassParameters::getParameters() const noexcept
{
    return parameters;
}

bool NESTypeClassParameters::empty() const noexcept
{
    return parameters.empty();
}

size_t NESTypeClassParameters::size() const noexcept
{
    return parameters.size();
}

NESParametrizedTypeClass NESParametrizedTypeClass::constant(NESTypeClass constantTypeClass)
{
    return NESParametrizedTypeClass{ConstantNESParametrizedTypeClass{.constantClass = std::move(constantTypeClass)}};
}

std::expected<NESTypeClass, Exception> NESParametrizedTypeClass::instantiate(const NESTypeClassParameters& params) const
{
    return self->instantiate(params);
}

const std::optional<ExpectedNESTypeArguments>& NESParametrizedTypeClass::getExpectedArguments() const noexcept
{
    return self->getExpectedArguments();
}

NESTypeClass NESParametrizedTypeClass::unparametrized() const
{
    auto result = instantiate(NESTypeClassParameters{});
    PRECONDITION(result.has_value(), "Failed to instantiate default type class: {}", result.error().what());
    return std::move(*result);
}

bool NESParametrizedTypeClass::operator==(const NESParametrizedTypeClass& other) const
{
    if (self == other.self)
    {
        return true;
    }
    return self->equals(*other.self);
}

NamedNESParametrizedTypeClass::NamedNESParametrizedTypeClass(QualifiedIdentifier name, NESParametrizedTypeClass underlying)
    : name(std::move(name)), underlying(std::move(underlying))
{
}

NamedNESParametrizedTypeClass NamedNESParametrizedTypeClass::constant(QualifiedIdentifier name, NESTypeClass constantTypeClass)
{
    return NamedNESParametrizedTypeClass{std::move(name), NESParametrizedTypeClass::constant(std::move(constantTypeClass))};
}

const QualifiedIdentifier& NamedNESParametrizedTypeClass::getFullyQualifiedName() const
{
    return name;
}

const NESParametrizedTypeClass& NamedNESParametrizedTypeClass::getUnderlying() const
{
    return underlying;
}

std::expected<NESTypeClass, Exception> NamedNESParametrizedTypeClass::instantiate(const NESTypeClassParameters& params) const
{
    return underlying.instantiate(params);
}

const std::optional<ExpectedNESTypeArguments>& NamedNESParametrizedTypeClass::getExpectedArguments() const noexcept
{
    return underlying.getExpectedArguments();
}

NESTypeClass NamedNESParametrizedTypeClass::unparametrized() const
{
    return underlying.unparametrized();
}

bool NamedNESParametrizedTypeClass::operator==(const NamedNESParametrizedTypeClass& other) const
{
    return name == other.name && underlying == other.underlying;
}

std::ostream& operator<<(std::ostream& os, const NamedNESParametrizedTypeClass& obj)
{
    return os << fmt::format("NamedNESParametrizedTypeClass({})", obj.name);
}

/// --- NESParameterId, NESTypeParameter, ExpectedNESTypeArguments & ExpectedNESValueArguments ---

/// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - a parameter instance's identity is its address
NESParameterId::NESParameterId(const NESTypeParameter& param) noexcept
    : parameterAddress(reinterpret_cast<uintptr_t>(std::addressof(param)))
{
}

/// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - a parameter instance's identity is its address
NESParameterId::NESParameterId(const NESOptionalTypeParameter& param) noexcept
    : parameterAddress(reinterpret_cast<uintptr_t>(std::addressof(param)))
{
}

/// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - a parameter instance's identity is its address
NESParameterId::NESParameterId(const NESFunctorParameter& param) noexcept
    : parameterAddress(reinterpret_cast<uintptr_t>(std::addressof(param)))
{
}

/// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - a parameter instance's identity is its address
NESParameterId::NESParameterId(const NESOptionalFunctorParameter& param) noexcept
    : parameterAddress(reinterpret_cast<uintptr_t>(std::addressof(param)))
{
}

std::ostream& operator<<(std::ostream& os, const NESParameterId& id)
{
    return os << fmt::format("NESParameterId(0x{:x})", id.parameterAddress);
}

NESTypeParameter::NESTypeParameter(Identifier name, std::string description, NESTypeClass constraint)
    : name(std::move(name))
    , description(std::move(description))
    , constraintSupplier([c = std::move(constraint)] { return c; })
    , defaultSupplier(std::nullopt)
{
}

NESTypeParameter::NESTypeParameter(Identifier name, std::string description, std::function<NESTypeClass()> constraintSupplier)
    : name(std::move(name))
    , description(std::move(description))
    , constraintSupplier(std::move(constraintSupplier))
    , defaultSupplier(std::nullopt)
{
}

NESTypeParameter::NESTypeParameter(Identifier name, std::string description, NESTypeClass constraint, NESType defaultType)
    : name(std::move(name))
    , description(std::move(description))
    , constraintSupplier([c = std::move(constraint)] { return c; })
    , defaultSupplier([d = std::move(defaultType)] { return d; })
{
}

NESTypeParameter::NESTypeParameter(
    Identifier name,
    std::string description,
    std::function<NESTypeClass()> constraintSupplier,
    std::function<NESType()> defaultSupplier)
    : name(std::move(name))
    , description(std::move(description))
    , constraintSupplier(std::move(constraintSupplier))
    , defaultSupplier(std::move(defaultSupplier))
{
}

NESOptionalTypeParameter::NESOptionalTypeParameter(Identifier name, std::string description, NESTypeClass constraint)
    : name(std::move(name))
    , description(std::move(description))
    , constraintSupplier([c = std::move(constraint)] { return c; })
{
}

NESOptionalTypeParameter::NESOptionalTypeParameter(
    Identifier name, std::string description, std::function<NESTypeClass()> constraintSupplier)
    : name(std::move(name))
    , description(std::move(description))
    , constraintSupplier(std::move(constraintSupplier))
{
}

NESFunctorParameter::NESFunctorParameter(Identifier name, std::string description)
    : name(std::move(name))
    , description(std::move(description))
    , validator([](const NESFunctor& f) -> std::expected<NESFunctor, Exception> { return f; })
    , defaultSupplier(std::nullopt)
{
}

NESFunctorParameter::NESFunctorParameter(
    Identifier name,
    std::string description,
    std::function<std::expected<NESFunctor, Exception>(const NESFunctor&)> validator)
    : name(std::move(name))
    , description(std::move(description))
    , validator(std::move(validator))
    , defaultSupplier(std::nullopt)
{
}

NESFunctorParameter::NESFunctorParameter(Identifier name, std::string description, NESFunctor defaultFunctor)
    : name(std::move(name))
    , description(std::move(description))
    , validator([](const NESFunctor& f) -> std::expected<NESFunctor, Exception> { return f; })
    , defaultSupplier([d = std::move(defaultFunctor)] { return d; })
{
}

NESFunctorParameter::NESFunctorParameter(Identifier name, std::string description, std::function<NESFunctor()> defaultSupplier)
    : name(std::move(name))
    , description(std::move(description))
    , validator([](const NESFunctor& f) -> std::expected<NESFunctor, Exception> { return f; })
    , defaultSupplier(std::move(defaultSupplier))
{
}

NESOptionalFunctorParameter::NESOptionalFunctorParameter(Identifier name, std::string description)
    : name(std::move(name))
    , description(std::move(description))
    , validator([](const NESFunctor& f) -> std::expected<NESFunctor, Exception> { return f; })
{
}

NESOptionalFunctorParameter::NESOptionalFunctorParameter(
    Identifier name,
    std::string description,
    std::function<std::expected<NESFunctor, Exception>(const NESFunctor&)> validator)
    : name(std::move(name))
    , description(std::move(description))
    , validator(std::move(validator))
{
}

QualifiedErasedNESTypeParameter::QualifiedErasedNESTypeParameter(
    QualifiedIdentifier name,
    const NESParameterId originalParameterId,
    const Kind kind,
    std::function<NESTypeClass()> constraintSupplier,
    std::function<std::expected<ExplicitAny, Exception>(const NESTypeParameters::Element&)> factory,
    std::optional<std::function<ExplicitAny()>> defaultSupplier,
    const bool optionalWithoutDefault,
    std::string description)
    : name(std::move(name))
    , originalParameterId(originalParameterId)
    , kind(kind)
    , constraintSupplier(std::move(constraintSupplier))
    , factory(std::move(factory))
    , defaultSupplier(std::move(defaultSupplier))
    , optionalWithoutDefault(optionalWithoutDefault)
    , description(std::move(description))
{
}

ExplicitAny QualifiedErasedNESTypeParameter::getDefault() const
{
    PRECONDITION(defaultSupplier.has_value(), "Parameter '{}' has no default supplier", name);
    return (*defaultSupplier)();
}

std::expected<ExplicitAny, Exception> QualifiedErasedNESTypeParameter::apply(const NESTypeParameters::Element& arg) const
{
    return factory(arg);
}

bool QualifiedErasedNESTypeParameter::operator==(const QualifiedErasedNESTypeParameter& other) const noexcept
{
    return name == other.name && originalParameterId == other.originalParameterId && kind == other.kind
        && optionalWithoutDefault == other.optionalWithoutDefault;
}

std::ostream& operator<<(std::ostream& os, const QualifiedErasedNESTypeParameter& obj)
{
    return os << fmt::format("QualifiedErasedNESTypeParameter({})", obj.name);
}

InstantiatedNESTypeParameter::InstantiatedNESTypeParameter(
    QualifiedIdentifier name, const NESParameterId originalParameterId, ExplicitAny value)
    : name(std::move(name)), originalParameterId(originalParameterId), value(std::move(value))
{
    PRECONDITION(this->value.hasValue(), "InstantiatedNESTypeParameter '{}' must hold a value", this->name);
}

bool InstantiatedNESTypeParameter::operator==(const InstantiatedNESTypeParameter& other) const noexcept
{
    return name == other.name && originalParameterId == other.originalParameterId;
}

std::ostream& operator<<(std::ostream& os, const InstantiatedNESTypeParameter& obj)
{
    return os << fmt::format("InstantiatedNESTypeParameter({})", obj.name);
}

InstantiatedNESTypeParameters::InstantiatedNESTypeParameters(const Schema<InstantiatedNESTypeParameter, Ordered>& entries)
{
    values.reserve(entries.size());
    for (const auto& entry : entries)
    {
        values.emplace(entry.getOriginalParameterId(), entry.getErasedValue());
    }
}

const ExplicitAny* InstantiatedNESTypeParameters::findRaw(const NESParameterId id) const noexcept
{
    if (const auto it = values.find(id); it != values.end())
    {
        return &it->second;
    }
    return nullptr;
}

NESType InstantiatedNESTypeParameters::get(const NESTypeParameter& param) const
{
    const auto* raw = findRaw(NESParameterId{param});
    PRECONDITION(raw != nullptr, "Could not find instantiated type for parameter '{}'", param.getName());
    return raw->getAs<NESType>();
}

std::optional<NESType> InstantiatedNESTypeParameters::get(const NESOptionalTypeParameter& param) const
{
    if (const auto* raw = findRaw(NESParameterId{param}))
    {
        return raw->getAs<NESType>();
    }
    return std::nullopt;
}

NESFunctor InstantiatedNESTypeParameters::get(const NESFunctorParameter& param) const
{
    const auto* raw = findRaw(NESParameterId{param});
    PRECONDITION(raw != nullptr, "Could not find instantiated functor for parameter '{}'", param.getName());
    return raw->getAs<NESFunctor>();
}

std::optional<NESFunctor> InstantiatedNESTypeParameters::get(const NESOptionalFunctorParameter& param) const
{
    if (const auto* raw = findRaw(NESParameterId{param}))
    {
        return raw->getAs<NESFunctor>();
    }
    return std::nullopt;
}

ExpectedNESTypeArguments::ExpectedNESTypeArguments(Schema<QualifiedErasedNESTypeParameter, Ordered> expectedParameters)
    : expectedParameters(std::move(expectedParameters))
{
    bool seenOptional = false;
    for (const auto& param : this->expectedParameters)
    {
        if (param.isOptional())
        {
            seenOptional = true;
        }
        else
        {
            PRECONDITION(
                !seenOptional,
                "Required functor parameter '{}' cannot follow an optional parameter",
                param.getFullyQualifiedName());
        }
    }
}

const Schema<QualifiedErasedNESTypeParameter, Ordered>& ExpectedNESTypeArguments::getParameters() const noexcept
{
    return expectedParameters;
}

size_t ExpectedNESTypeArguments::size() const noexcept
{
    return expectedParameters.size();
}

bool ExpectedNESTypeArguments::empty() const noexcept
{
    return expectedParameters.size() == 0;
}

std::expected<InstantiatedNESTypeParameters, Exception> ExpectedNESTypeArguments::resolve(const NESTypeParameters& passed) const
{
    const auto requiredCount = std::ranges::count_if(
        expectedParameters, [](const QualifiedErasedNESTypeParameter& param) { return !param.isOptional(); });

    std::vector<InstantiatedNESTypeParameter> instantiated;
    instantiated.reserve(expectedParameters.size());

    if (!passed.isNamed())
    {
        if (passed.size() < static_cast<size_t>(requiredCount) || passed.size() > expectedParameters.size())
        {
            return std::unexpected{TypeInferenceException(
                "Functor expects between {} and {} argument(s), but got {}", requiredCount, expectedParameters.size(), passed.size())};
        }

        for (size_t i = 0; i < expectedParameters.size(); ++i)
        {
            const auto expectedParam = expectedParameters[i].value();
            if (i < passed.size())
            {
                auto applied = expectedParam.apply(passed.at(i).value());
                if (!applied.has_value())
                {
                    return std::unexpected{std::move(applied).error()};
                }
                instantiated.emplace_back(
                    expectedParam.getFullyQualifiedName(), expectedParam.getOriginalParameterId(), std::move(*applied));
            }
            else if (expectedParam.hasDefault())
            {
                instantiated.emplace_back(
                    expectedParam.getFullyQualifiedName(), expectedParam.getOriginalParameterId(), expectedParam.getDefault());
            }
        }
        return InstantiatedNESTypeParameters{Schema<InstantiatedNESTypeParameter, Ordered>{std::move(instantiated)}};
    }

    const auto* namedSchema = passed.tryGetNamedSchema();
    INVARIANT(namedSchema != nullptr, "Expected named NESTypeParameters");
    if (const auto dup = findDuplicateFieldName(*namedSchema); dup.has_value())
    {
        return std::unexpected{TypeInferenceException("Duplicate named parameter '{}' passed to functor", *dup)};
    }

    std::unordered_set<NESParameterId, detail::NESParameterIdHash> matchedIds;
    std::unordered_map<NESParameterId, ExplicitAny, detail::NESParameterIdHash> matchedValues;

    for (const auto& passedParam : *namedSchema)
    {
        auto targetOpt = findExactNamedField(expectedParameters, passedParam.getFullyQualifiedName());
        if (!targetOpt.has_value())
        {
            targetOpt = expectedParameters.getFieldByName(passedParam.getFullyQualifiedName());
        }
        if (!targetOpt.has_value())
        {
            return std::unexpected{
                TypeInferenceException("Unknown named argument '{}' passed to functor", passedParam.getFullyQualifiedName())};
        }
        if (!matchedIds.insert(targetOpt->getOriginalParameterId()).second)
        {
            return std::unexpected{
                TypeInferenceException("Duplicate named parameter '{}' passed to functor", targetOpt->getFullyQualifiedName())};
        }
        auto applied = targetOpt->apply(passedParam.getUnderlying());
        if (!applied.has_value())
        {
            return std::unexpected{std::move(applied).error()};
        }
        matchedValues.emplace(targetOpt->getOriginalParameterId(), std::move(*applied));
    }

    for (const auto& expectedParam : expectedParameters)
    {
        if (const auto it = matchedValues.find(expectedParam.getOriginalParameterId()); it != matchedValues.end())
        {
            instantiated.emplace_back(
                expectedParam.getFullyQualifiedName(), expectedParam.getOriginalParameterId(), it->second);
        }
        else if (expectedParam.hasDefault())
        {
            instantiated.emplace_back(
                expectedParam.getFullyQualifiedName(), expectedParam.getOriginalParameterId(), expectedParam.getDefault());
        }
        else if (!expectedParam.isOptional())
        {
            return std::unexpected{TypeInferenceException("Missing required parameter '{}'", expectedParam.getFullyQualifiedName())};
        }
    }

    return InstantiatedNESTypeParameters{Schema<InstantiatedNESTypeParameter, Ordered>{std::move(instantiated)}};
}

QualifiedErasedNESValueParameter::QualifiedErasedNESValueParameter(
    QualifiedIdentifier name,
    const NESParameterId originalParameterId,
    std::function<NESTypeClass()> constraintSupplier,
    std::function<std::expected<ExplicitAny, Exception>(const NESValue&)> factory,
    std::optional<std::function<ExplicitAny()>> defaultSupplier,
    std::string description)
    : name(std::move(name))
    , originalParameterId(originalParameterId)
    , constraintSupplier(std::move(constraintSupplier))
    , factory(std::move(factory))
    , defaultSupplier(std::move(defaultSupplier))
    , description(std::move(description))
{
}

ExplicitAny QualifiedErasedNESValueParameter::getDefault() const
{
    PRECONDITION(defaultSupplier.has_value(), "Value parameter '{}' has no default supplier", name);
    return (*defaultSupplier)();
}

std::expected<ExplicitAny, Exception> QualifiedErasedNESValueParameter::apply(const NESValue& val) const
{
    return factory(val);
}

bool QualifiedErasedNESValueParameter::operator==(const QualifiedErasedNESValueParameter& other) const noexcept
{
    return name == other.name && originalParameterId == other.originalParameterId;
}

std::ostream& operator<<(std::ostream& os, const QualifiedErasedNESValueParameter& obj)
{
    return os << fmt::format("QualifiedErasedNESValueParameter({})", obj.name);
}

InstantiatedNESValueParameter::InstantiatedNESValueParameter(
    QualifiedIdentifier name, const NESParameterId originalParameterId, ExplicitAny value)
    : name(std::move(name)), originalParameterId(originalParameterId), value(std::move(value))
{
    PRECONDITION(this->value.hasValue(), "InstantiatedNESValueParameter '{}' must hold a value", this->name);
}

bool InstantiatedNESValueParameter::operator==(const InstantiatedNESValueParameter& other) const noexcept
{
    return name == other.name && originalParameterId == other.originalParameterId;
}

std::ostream& operator<<(std::ostream& os, const InstantiatedNESValueParameter& obj)
{
    return os << fmt::format("InstantiatedNESValueParameter({})", obj.name);
}

InstantiatedNESValueParameters::InstantiatedNESValueParameters(const Schema<InstantiatedNESValueParameter, Ordered>& entries)
{
    values.reserve(entries.size());
    for (const auto& entry : entries)
    {
        values.emplace(entry.getOriginalParameterId(), entry.getErasedValue());
    }
}

const ExplicitAny* InstantiatedNESValueParameters::findRaw(const NESParameterId id) const noexcept
{
    if (const auto it = values.find(id); it != values.end())
    {
        return &it->second;
    }
    return nullptr;
}

ExpectedNESValueArguments::ExpectedNESValueArguments(Schema<QualifiedErasedNESValueParameter, Ordered> expectedParameters)
    : expectedParameters(std::move(expectedParameters))
{
    bool seenOptional = false;
    for (const auto& param : this->expectedParameters)
    {
        if (param.hasDefault())
        {
            seenOptional = true;
        }
        else
        {
            PRECONDITION(
                !seenOptional,
                "Required value parameter '{}' cannot follow an optional parameter",
                param.getFullyQualifiedName());
        }
    }
}

const Schema<QualifiedErasedNESValueParameter, Ordered>& ExpectedNESValueArguments::getParameters() const noexcept
{
    return expectedParameters;
}

size_t ExpectedNESValueArguments::size() const noexcept
{
    return expectedParameters.size();
}

bool ExpectedNESValueArguments::empty() const noexcept
{
    return expectedParameters.size() == 0;
}

std::expected<InstantiatedNESValueParameters, Exception> ExpectedNESValueArguments::resolve(const NESValueParameters& passed) const
{
    const auto requiredCount = std::ranges::count_if(
        expectedParameters, [](const QualifiedErasedNESValueParameter& param) { return !param.hasDefault(); });

    std::vector<InstantiatedNESValueParameter> instantiated;
    instantiated.reserve(expectedParameters.size());

    if (!passed.isNamed())
    {
        if (passed.size() < static_cast<size_t>(requiredCount) || passed.size() > expectedParameters.size())
        {
            return std::unexpected{InvalidQuerySyntax(
                "Constructor expects between {} and {} argument(s), but got {}", requiredCount, expectedParameters.size(), passed.size())};
        }

        for (size_t i = 0; i < expectedParameters.size(); ++i)
        {
            const auto expectedParam = expectedParameters[i].value();
            if (i < passed.size())
            {
                auto applied = expectedParam.apply(passed.at(i).value());
                if (!applied.has_value())
                {
                    return std::unexpected{std::move(applied).error()};
                }
                instantiated.emplace_back(
                    expectedParam.getFullyQualifiedName(), expectedParam.getOriginalParameterId(), std::move(*applied));
            }
            else if (expectedParam.hasDefault())
            {
                instantiated.emplace_back(
                    expectedParam.getFullyQualifiedName(), expectedParam.getOriginalParameterId(), expectedParam.getDefault());
            }
        }
        return InstantiatedNESValueParameters{Schema<InstantiatedNESValueParameter, Ordered>{std::move(instantiated)}};
    }

    const auto* namedSchema = passed.tryGetNamedSchema();
    INVARIANT(namedSchema != nullptr, "Expected named NESValueParameters");
    if (const auto dup = findDuplicateFieldName(*namedSchema); dup.has_value())
    {
        return std::unexpected{InvalidConfigParameter("Duplicate named argument '{}'", *dup)};
    }

    std::unordered_set<NESParameterId, detail::NESParameterIdHash> matchedIds;
    std::unordered_map<NESParameterId, ExplicitAny, detail::NESParameterIdHash> matchedValues;

    for (const auto& passedVal : *namedSchema)
    {
        auto targetOpt = findExactNamedField(expectedParameters, passedVal.getFullyQualifiedName());
        if (!targetOpt.has_value())
        {
            targetOpt = expectedParameters.getFieldByName(passedVal.getFullyQualifiedName());
        }
        if (!targetOpt.has_value())
        {
            return std::unexpected{InvalidQuerySyntax("Unknown named argument '{}'", passedVal.getFullyQualifiedName())};
        }
        if (!matchedIds.insert(targetOpt->getOriginalParameterId()).second)
        {
            return std::unexpected{InvalidConfigParameter("Duplicate named argument '{}'", targetOpt->getFullyQualifiedName())};
        }
        auto applied = targetOpt->apply(passedVal.getValue());
        if (!applied.has_value())
        {
            return std::unexpected{std::move(applied).error()};
        }
        matchedValues.emplace(targetOpt->getOriginalParameterId(), std::move(*applied));
    }

    for (const auto& expectedParam : expectedParameters)
    {
        if (const auto it = matchedValues.find(expectedParam.getOriginalParameterId()); it != matchedValues.end())
        {
            instantiated.emplace_back(
                expectedParam.getFullyQualifiedName(), expectedParam.getOriginalParameterId(), it->second);
        }
        else if (expectedParam.hasDefault())
        {
            instantiated.emplace_back(
                expectedParam.getFullyQualifiedName(), expectedParam.getOriginalParameterId(), expectedParam.getDefault());
        }
        else
        {
            return std::unexpected{InvalidQuerySyntax("Missing required argument '{}'", expectedParam.getFullyQualifiedName())};
        }
    }

    return InstantiatedNESValueParameters{Schema<InstantiatedNESValueParameter, Ordered>{std::move(instantiated)}};
}

/// --- NESFunctor & NamedNESFunctor ---

std::expected<NESType, Exception> NESFunctor::instantiate(const NESTypeParameters& params) const
{
    return self->instantiateRaw(params);
}

std::expected<NESType, Exception> NESFunctor::instantiate() const
{
    return instantiate(NESTypeParameters{});
}

const std::optional<ExpectedNESTypeArguments>& NESFunctor::getExpectedArguments() const noexcept
{
    return self->getExpectedArguments();
}

bool NESFunctor::operator==(const NESFunctor& other) const
{
    if (self == other.self)
    {
        return true;
    }
    return self->equals(*other.self);
}

NamedNESFunctor::NamedNESFunctor(QualifiedIdentifier name, NESFunctor underlyingFunctor)
    : name(std::move(name)), underlyingFunctor(std::move(underlyingFunctor))
{
}

const QualifiedIdentifier& NamedNESFunctor::getFullyQualifiedName() const
{
    return name;
}

const NESFunctor& NamedNESFunctor::getUnderlyingFunctor() const
{
    return underlyingFunctor;
}

std::expected<NESType, Exception> NamedNESFunctor::instantiate(const NESTypeParameters& params) const
{
    return underlyingFunctor.instantiate(params);
}

std::expected<NESType, Exception> NamedNESFunctor::instantiate() const
{
    return underlyingFunctor.instantiate();
}

bool NamedNESFunctor::operator==(const NamedNESFunctor& other) const
{
    return name == other.name && underlyingFunctor == other.underlyingFunctor;
}

std::ostream& operator<<(std::ostream& os, const NamedNESFunctor& obj)
{
    return os << fmt::format("NamedNESFunctor({})", obj.name);
}

}
