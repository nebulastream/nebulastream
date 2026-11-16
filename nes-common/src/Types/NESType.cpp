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
#include <functional>
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

namespace
{
struct AnyTypeClassTag
{
    bool operator==(const AnyTypeClassTag&) const = default;
};

struct ConjunctionTypeClassDescriptor
{
    NESTypeClass left;
    NESTypeClass right;
    bool operator==(const ConjunctionTypeClassDescriptor&) const = default;
};

struct DisjunctionTypeClassDescriptor
{
    NESTypeClass left;
    NESTypeClass right;
    bool operator==(const DisjunctionTypeClassDescriptor&) const = default;
};

struct ConstantParametrizedTypeClassDescriptor
{
    NESTypeClass constantClass;
    bool operator==(const ConstantParametrizedTypeClassDescriptor&) const = default;
};
}

/// --- NESType & NamedNESType ---

NESType::NESType(std::shared_ptr<const detail::NESTypeImpl> impl) : impl(std::move(impl))
{
    PRECONDITION(this->impl != nullptr, "NESType implementation pointer must not be null");
}

NESType::NESType(
    ExplicitAny typeDescriptor,
    detail::NESTypeImpl::RawValueConstructor valueConstructor,
    detail::NESTypeImpl::RawBinaryPred typeEquals,
    detail::NESTypeImpl::RawBinaryPred valueEquals)
    : impl(std::make_shared<detail::NESTypeImpl>(detail::NESTypeImpl{
          .typeDescriptor = std::move(typeDescriptor),
          .traits = {},
          .valueConstructor = std::move(valueConstructor),
          .typeEquals = std::move(typeEquals),
          .valueEquals = std::move(valueEquals)}))
{
}

NESType::NESType(NamedNESType namedType)
    : NESType(NESType::createUnconstructible<NamedNESType>(std::move(namedType)))
{
}

std::expected<NESValue, Exception> NESType::construct(const NESValueParameters& params) const
{
    return impl->valueConstructor(impl->typeDescriptor, params)
        .transform([this](ExplicitAny&& representation) { return NESValue{*this, std::move(representation)}; });
}

std::expected<NESValue, Exception> NESType::construct() const
{
    return construct(NESValueParameters{});
}

bool NESType::operator==(const NESType& other) const
{
    if (impl == other.impl)
    {
        return true;
    }
    return impl->typeEquals(impl->typeDescriptor, other.impl->typeDescriptor);
}

bool NESType::equalsValue(const ExplicitAny& lhs, const ExplicitAny& rhs) const
{
    return impl->valueEquals(lhs, rhs);
}

const ExplicitAny& NESType::getDescriptor() const
{
    return impl->typeDescriptor;
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
    : type(std::move(type)), internalRepresentation(std::move(internalRepresentation))
{
}

const ExplicitAny& NESValue::getInternalRepresentation() const
{
    return internalRepresentation;
}

const NESType& NESValue::getType() const
{
    return type;
}

bool NESValue::operator==(const NESValue& other) const
{
    return type == other.type && type.equalsValue(internalRepresentation, other.internalRepresentation);
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

std::expected<Schema<NamedNESValue, Ordered>, Exception>
NESValueParameters::bindAgainst(const std::span<const QualifiedIdentifier> formalNames) const
{
    if (const auto* pos = std::get_if<std::vector<NESValue>>(&storage))
    {
        if (pos->size() != formalNames.size())
        {
            return std::unexpected{
                InvalidQuerySyntax("Expected {} argument(s), but got {}", formalNames.size(), pos->size())};
        }
        std::vector<NamedNESValue> bound;
        bound.reserve(formalNames.size());
        for (size_t i = 0; i < formalNames.size(); ++i)
        {
            bound.emplace_back(formalNames[i], (*pos)[i]);
        }
        return Schema<NamedNESValue, Ordered>{std::move(bound)};
    }

    const auto& namedSchema = std::get<Schema<NamedNESValue, Ordered>>(storage);
    if (namedSchema.size() != formalNames.size())
    {
        return std::unexpected{
            InvalidQuerySyntax("Expected {} named argument(s), but got {}", formalNames.size(), namedSchema.size())};
    }

    std::vector<NamedNESValue> orderedBound;
    orderedBound.reserve(formalNames.size());
    for (const auto& formalName : formalNames)
    {
        const auto found = namedSchema.getFieldByName(formalName);
        if (!found.has_value())
        {
            return std::unexpected{InvalidQuerySyntax("Missing required argument '{}'", formalName)};
        }
        orderedBound.emplace_back(formalName, found->getValue());
    }
    return Schema<NamedNESValue, Ordered>{std::move(orderedBound)};
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

NamedNESTypeParameter::NamedNESTypeParameter(QualifiedIdentifier name, std::variant<NESType, NESValue> param)
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

const NESType* NamedNESTypeParameter::tryGetType() const noexcept
{
    return std::get_if<NESType>(&value);
}

const NESValue* NamedNESTypeParameter::tryGetValue() const noexcept
{
    return std::get_if<NESValue>(&value);
}

const std::variant<NESType, NESValue>& NamedNESTypeParameter::getUnderlying() const noexcept
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

std::expected<Schema<NamedNESTypeParameter, Ordered>, Exception>
NESTypeParameters::bindAgainst(const std::span<const QualifiedIdentifier> formalNames) const
{
    if (const auto* pos = std::get_if<std::vector<Element>>(&storage))
    {
        if (pos->size() != formalNames.size())
        {
            return std::unexpected{
                TypeInferenceException("Expected {} type argument(s), but got {}", formalNames.size(), pos->size())};
        }
        std::vector<NamedNESTypeParameter> bound;
        bound.reserve(formalNames.size());
        for (size_t i = 0; i < formalNames.size(); ++i)
        {
            bound.emplace_back(formalNames[i], (*pos)[i]);
        }
        return Schema<NamedNESTypeParameter, Ordered>{std::move(bound)};
    }

    const auto& namedSchema = std::get<Schema<NamedNESTypeParameter, Ordered>>(storage);
    if (namedSchema.size() != formalNames.size())
    {
        return std::unexpected{
            TypeInferenceException("Expected {} named type argument(s), but got {}", formalNames.size(), namedSchema.size())};
    }

    std::vector<NamedNESTypeParameter> orderedBound;
    orderedBound.reserve(formalNames.size());
    for (const auto& formalName : formalNames)
    {
        const auto found = namedSchema.getFieldByName(formalName);
        if (!found.has_value())
        {
            return std::unexpected{TypeInferenceException("Missing required type argument '{}'", formalName)};
        }
        orderedBound.push_back(*found);
    }
    return Schema<NamedNESTypeParameter, Ordered>{std::move(orderedBound)};
}

bool NESTypeParameters::operator==(const NESTypeParameters& other) const
{
    return storage == other.storage;
}

/// --- NESTypeClass & Parameterized Type Classes ---

NESTypeClass::NESTypeClass(
    ExplicitAny typeClassDescriptor,
    std::function<bool(const ExplicitAny&, const NESType&)> predicate,
    std::function<bool(const ExplicitAny&, const ExplicitAny&)> typeClassEquals)
    : typeClassDescriptor(std::move(typeClassDescriptor))
    , predicate(std::move(predicate))
    , typeClassEquals(std::move(typeClassEquals))
{
}

NESTypeClass NESTypeClass::any()
{
    return NESTypeClass{AnyTypeClassTag{}, [](const AnyTypeClassTag&, const NESType&) { return true; }};
}

bool NESTypeClass::validate(const NESType& type) const
{
    return predicate(typeClassDescriptor, type);
}

bool NESTypeClass::operator==(const NESTypeClass& other) const
{
    return typeClassEquals(typeClassDescriptor, other.typeClassDescriptor);
}

NESTypeClass operator&&(const NESTypeClass& lhs, const NESTypeClass& rhs)
{
    return NESTypeClass{
        ConjunctionTypeClassDescriptor{.left = lhs, .right = rhs},
        [](const ConjunctionTypeClassDescriptor& desc, const NESType& type)
        { return desc.left.validate(type) && desc.right.validate(type); }};
}

NESTypeClass operator||(const NESTypeClass& lhs, const NESTypeClass& rhs)
{
    return NESTypeClass{
        DisjunctionTypeClassDescriptor{.left = lhs, .right = rhs},
        [](const DisjunctionTypeClassDescriptor& desc, const NESType& type)
        { return desc.left.validate(type) || desc.right.validate(type); }};
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
    return NESParametrizedTypeClass{
        ConstantParametrizedTypeClassDescriptor{.constantClass = std::move(constantTypeClass)},
        [](const ConstantParametrizedTypeClassDescriptor& desc, const NESTypeClassParameters& params)
            -> std::expected<NESTypeClass, Exception>
        {
            if (!params.empty())
            {
                return std::unexpected{TypeInferenceException(
                    "Unparameterized type class expects 0 arguments, but got {}", params.size())};
            }
            return desc.constantClass;
        }};
}

std::expected<NESTypeClass, Exception> NESParametrizedTypeClass::instantiate(const NESTypeClassParameters& params) const
{
    return typeClassConstructor(structure, params);
}

NESTypeClass NESParametrizedTypeClass::unparametrized() const
{
    auto result = instantiate(NESTypeClassParameters{});
    PRECONDITION(result.has_value(), "Failed to instantiate default type class: {}", result.error().what());
    return std::move(*result);
}

bool NESParametrizedTypeClass::operator==(const NESParametrizedTypeClass& other) const
{
    return structureEquals(structure, other.structure);
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

/// --- ExpectedNESTypeArguments ---

std::ostream& operator<<(std::ostream& os, const ExpectedNESTypeParameter& obj)
{
    return os << fmt::format("ExpectedNESTypeParameter({})", obj.name);
}

ExpectedNESTypeArguments::ExpectedNESTypeArguments(Schema<ExpectedNESTypeParameter, Ordered> expectedParameters)
    : expectedParameters(std::move(expectedParameters))
{
}

ExpectedNESTypeArguments::ExpectedNESTypeArguments(const Schema<NamedNESTypeClass, Ordered>& expectedTypeClasses)
    : expectedParameters(
          expectedTypeClasses
          | std::views::transform(
              [](const NamedNESTypeClass& namedTc)
              {
                  return ExpectedNESTypeParameter{
                      .name = namedTc.getFullyQualifiedName(),
                      .constraint = namedTc.getTypeClass(),
                      .kind = ExpectedNESTypeParameter::Kind::TypeParameter,
                      .optional = false};
              })
          | std::ranges::to<Schema<ExpectedNESTypeParameter, Ordered>>())
{
}

const Schema<ExpectedNESTypeParameter, Ordered>& ExpectedNESTypeArguments::getParameters() const noexcept
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

std::expected<NESTypeParameters, Exception> ExpectedNESTypeArguments::validateAndBind(const NESTypeParameters& passed) const
{
    const auto requiredCount = std::ranges::count_if(
        expectedParameters, [](const ExpectedNESTypeParameter& param) { return !param.optional; });

    if (!passed.isNamed())
    {
        if (passed.size() < static_cast<size_t>(requiredCount) || passed.size() > expectedParameters.size())
        {
            return std::unexpected{TypeInferenceException(
                "Functor expects between {} and {} argument(s), but got {}",
                requiredCount,
                expectedParameters.size(),
                passed.size())};
        }

        std::vector<NamedNESTypeParameter> bound;
        bound.reserve(passed.size());
        for (size_t i = 0; i < passed.size(); ++i)
        {
            const auto expectedParam = expectedParameters[i].value();
            const auto elem = passed.at(i).value();
            if (expectedParam.kind == ExpectedNESTypeParameter::Kind::TypeParameter)
            {
                const auto* typePtr = std::get_if<NESType>(&elem);
                if (typePtr == nullptr)
                {
                    return std::unexpected{TypeInferenceException(
                        "Parameter '{}' expects a type argument, but received a value", expectedParam.name)};
                }
                if (!expectedParam.constraint.validate(*typePtr))
                {
                    return std::unexpected{TypeInferenceException(
                        "Type argument for parameter '{}' does not satisfy expected type class constraint", expectedParam.name)};
                }
            }
            else
            {
                const auto* valPtr = std::get_if<NESValue>(&elem);
                if (valPtr == nullptr)
                {
                    return std::unexpected{TypeInferenceException(
                        "Parameter '{}' expects a constant value argument, but received a type", expectedParam.name)};
                }
                if (!expectedParam.constraint.validate(valPtr->getType()))
                {
                    return std::unexpected{TypeInferenceException(
                        "Value argument for parameter '{}' has invalid type", expectedParam.name)};
                }
            }
            bound.emplace_back(expectedParam.name, elem);
        }
        return NESTypeParameters::named(Schema<NamedNESTypeParameter, Ordered>{std::move(bound)});
    }

    std::unordered_set<QualifiedIdentifier> consumed;
    std::vector<NamedNESTypeParameter> bound;
    for (const auto& expectedParam : expectedParameters)
    {
        const auto found = passed.getByName(expectedParam.name);
        if (!found.has_value())
        {
            if (expectedParam.optional)
            {
                continue;
            }
            return std::unexpected{TypeInferenceException("Missing required parameter '{}'", expectedParam.name)};
        }
        consumed.insert(expectedParam.name);
        if (expectedParam.kind == ExpectedNESTypeParameter::Kind::TypeParameter)
        {
            if (!found->isType())
            {
                return std::unexpected{TypeInferenceException(
                    "Parameter '{}' expects a type argument, but received a value", expectedParam.name)};
            }
            if (!expectedParam.constraint.validate(*found->tryGetType()))
            {
                return std::unexpected{TypeInferenceException(
                    "Type argument for parameter '{}' does not satisfy expected type class constraint", expectedParam.name)};
            }
        }
        else
        {
            if (!found->isValue())
            {
                return std::unexpected{TypeInferenceException(
                    "Parameter '{}' expects a constant value argument, but received a type", expectedParam.name)};
            }
            if (!expectedParam.constraint.validate(found->tryGetValue()->getType()))
            {
                return std::unexpected{TypeInferenceException(
                    "Value argument for parameter '{}' has invalid type", expectedParam.name)};
            }
        }
        bound.push_back(*found);
    }

    if (consumed.size() != passed.size())
    {
        return std::unexpected{TypeInferenceException(
            "Unknown named argument(s) passed to functor (expected at most {} known arguments, got {})",
            expectedParameters.size(),
            passed.size())};
    }

    return NESTypeParameters::named(Schema<NamedNESTypeParameter, Ordered>{std::move(bound)});
}

/// --- NESFunctor & NamedNESFunctor ---

std::expected<NESType, Exception> NESFunctor::instantiate(const NESTypeParameters& params) const
{
    if (expectedArguments.has_value())
    {
        auto validated = expectedArguments->validateAndBind(params);
        if (!validated.has_value())
        {
            return std::unexpected{std::move(validated).error()};
        }
        return typeConstructor(structure, *validated);
    }
    return typeConstructor(structure, params);
}

const std::optional<ExpectedNESTypeArguments>& NESFunctor::getExpectedArguments() const noexcept
{
    return expectedArguments;
}

const ExplicitAny& NESFunctor::getStructure() const noexcept
{
    return structure;
}

bool NESFunctor::operator==(const NESFunctor& other) const
{
    return typeStructureEquals(structure, other.structure);
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

bool NamedNESFunctor::operator==(const NamedNESFunctor& other) const
{
    return name == other.name && underlyingFunctor == other.underlyingFunctor;
}

std::ostream& operator<<(std::ostream& os, const NamedNESFunctor& obj)
{
    return os << fmt::format("NamedNESFunctor({})", obj.name);
}

}
