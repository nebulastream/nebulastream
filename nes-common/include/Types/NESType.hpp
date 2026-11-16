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
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <functional>
#include <memory>
#include <optional>
#include <ostream>
#include <span>
#include <string_view>
#include <type_traits>
#include <typeindex>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <Identifiers/QualifiedIdentifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Any.hpp>
#include <Util/Logger/Formatter.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

class NESType;
class NESValue;
class NamedNESValue;
class NESValueParameters;
class NamedNESType;
class NamedNESTypeParameter;
class NESTypeParameters;
class NESTypeClass;
class NamedNESTypeClass;
class NESTypeClassParameters;
class NESParametrizedTypeClass;
class NamedNESParametrizedTypeClass;
class ExpectedNESTypeArguments;
class NESFunctor;
class NamedNESFunctor;

namespace detail
{
struct NESTypeImpl
{
    using TraitMap = std::unordered_map<std::type_index, ExplicitAny>;
    using RawValueConstructor = std::function<std::expected<ExplicitAny, Exception>(const ExplicitAny&, const NESValueParameters&)>;
    using RawBinaryPred = std::function<bool(const ExplicitAny&, const ExplicitAny&)>;

    ExplicitAny typeDescriptor;
    TraitMap traits;
    RawValueConstructor valueConstructor;
    RawBinaryPred typeEquals;
    RawBinaryPred valueEquals;
};
}

/// A fully instantiated type (comparable to a fully instantiated template in C++).
/// For example, Array<VARCHAR, 5>, DataType(INT32, NOT_NULLABLE), or OpenVinoModel(inputs, outputs).
/// Implemented as an immutable flyweight handle (`std::shared_ptr<const detail::NESTypeImpl>`) so
/// copying `NESType` and `NESValue` is O(1) and free of self-referential closure cycles.
class NESType
{
    std::shared_ptr<const detail::NESTypeImpl> impl;

    explicit NESType(std::shared_ptr<const detail::NESTypeImpl> impl);

public:
    NESType(
        ExplicitAny typeDescriptor,
        detail::NESTypeImpl::RawValueConstructor valueConstructor,
        detail::NESTypeImpl::RawBinaryPred typeEquals,
        detail::NESTypeImpl::RawBinaryPred valueEquals);

    template <typename Descriptor, typename ValueRepr>
    static NESType create(
        Descriptor descriptor,
        std::type_identity_t<std::function<std::expected<ValueRepr, Exception>(const Descriptor&, const NESValueParameters&)>> constructor,
        std::type_identity_t<std::function<bool(const Descriptor&, const Descriptor&)>> typeEquals =
            [](const Descriptor& lhs, const Descriptor& rhs)
        {
            if constexpr (std::equality_comparable<Descriptor>)
            {
                return lhs == rhs;
            }
            else
            {
                return &lhs == &rhs;
            }
        })
    {
        return NESType{
            ExplicitAny::create(std::move(descriptor)),
            [constructor = std::move(constructor)](
                const ExplicitAny& descAny, const NESValueParameters& params) -> std::expected<ExplicitAny, Exception>
            {
                const auto* casted = descAny.tryGetAs<Descriptor>();
                PRECONDITION(casted != nullptr, "NESType descriptor type mismatch during value construction");
                return constructor(*casted, params).transform([](ValueRepr&& repr) { return ExplicitAny::create(std::move(repr)); });
            },
            [typeEquals = std::move(typeEquals)](const ExplicitAny& lhs, const ExplicitAny& rhs)
            {
                const auto* lhsCasted = lhs.tryGetAs<Descriptor>();
                const auto* rhsCasted = rhs.tryGetAs<Descriptor>();
                if (lhsCasted != nullptr && rhsCasted != nullptr)
                {
                    return typeEquals(*lhsCasted, *rhsCasted);
                }
                return false;
            },
            [](const ExplicitAny& lhs, const ExplicitAny& rhs)
            {
                const auto* lhsCasted = lhs.tryGetAs<ValueRepr>();
                const auto* rhsCasted = rhs.tryGetAs<ValueRepr>();
                if (lhsCasted != nullptr && rhsCasted != nullptr)
                {
                    if constexpr (std::equality_comparable<ValueRepr>)
                    {
                        return *lhsCasted == *rhsCasted;
                    }
                    else
                    {
                        return lhsCasted == rhsCasted;
                    }
                }
                return false;
            }};
    }

    template <typename Descriptor, typename ValueRepr = Descriptor>
    static NESType createUnconstructible(
        Descriptor descriptor,
        std::type_identity_t<std::function<bool(const Descriptor&, const Descriptor&)>> typeEquals =
            [](const Descriptor& lhs, const Descriptor& rhs)
        {
            if constexpr (std::equality_comparable<Descriptor>)
            {
                return lhs == rhs;
            }
            else
            {
                return &lhs == &rhs;
            }
        })
    {
        return create<Descriptor, ValueRepr>(
            std::move(descriptor),
            [](const Descriptor&, const NESValueParameters&) -> std::expected<ValueRepr, Exception>
            { return std::unexpected{TypeInferenceException("Values of this NESType cannot be constructed directly from arguments")}; },
            std::move(typeEquals));
    }

    explicit NESType(NamedNESType namedType);

    template <typename Trait>
    [[nodiscard]] NESType withTrait(Trait trait) const
    {
        using CleanTrait = std::remove_cvref_t<Trait>;
        auto copy = std::make_shared<detail::NESTypeImpl>(*impl);
        copy->traits.insert_or_assign(std::type_index{typeid(CleanTrait)}, ExplicitAny::create(CleanTrait{std::move(trait)}));
        return NESType{std::move(copy)};
    }

    template <typename Trait>
    [[nodiscard]] const std::remove_cvref_t<Trait>* tryGetTrait() const noexcept
    {
        using CleanTrait = std::remove_cvref_t<Trait>;
        if (const auto it = impl->traits.find(std::type_index{typeid(CleanTrait)}); it != impl->traits.end())
        {
            return it->second.tryGetAs<CleanTrait>();
        }
        return nullptr;
    }

    template <typename Trait>
    [[nodiscard]] bool hasTrait() const noexcept
    {
        return tryGetTrait<Trait>() != nullptr;
    }

    [[nodiscard]] std::expected<NESValue, Exception> construct(const NESValueParameters& params) const;
    [[nodiscard]] std::expected<NESValue, Exception> construct() const;

    [[nodiscard]] bool operator==(const NESType& other) const;
    [[nodiscard]] bool equalsValue(const ExplicitAny& lhs, const ExplicitAny& rhs) const;

    [[nodiscard]] const ExplicitAny& getDescriptor() const;

    template <typename T>
    [[nodiscard]] const std::remove_cvref_t<T>* tryGetDescriptorAs() const noexcept
    {
        return impl->typeDescriptor.tryGetAs<T>();
    }
};

/// A type with an explicit name, considered NOT equal to a type with the same structure but a different name.
class NamedNESType
{
    QualifiedIdentifier name;
    NESType underlying;

public:
    NamedNESType(QualifiedIdentifier name, NESType underlying);

    [[nodiscard]] const QualifiedIdentifier& getFullyQualifiedName() const;
    [[nodiscard]] const NESType& getUnderlyingType() const;
    [[nodiscard]] bool operator==(const NamedNESType& other) const;
    friend std::ostream& operator<<(std::ostream& os, const NamedNESType& obj);
};

/// An actual runtime/compile-time instance of some `NESType` (e.g. a `LogicalOperator`, `LogicalFunction`, `RegisteredModel`, or literal).
class NESValue
{
    NESType type;
    ExplicitAny internalRepresentation;

public:
    NESValue(NESType type, ExplicitAny internalRepresentation);

    template <typename T>
    static NESValue create(NESType type, T&& repr)
    {
        return NESValue{std::move(type), ExplicitAny::create(std::forward<T>(repr))};
    }

    [[nodiscard]] const ExplicitAny& getInternalRepresentation() const;
    [[nodiscard]] const NESType& getType() const;

    template <typename T>
    [[nodiscard]] const std::remove_cvref_t<T>* tryGetAs() const noexcept
    {
        return internalRepresentation.tryGetAs<T>();
    }

    template <typename T>
    [[nodiscard]] T getAs() const
    {
        return internalRepresentation.getAs<T>();
    }

    [[nodiscard]] bool operator==(const NESValue& other) const;
};

class NamedNESValue
{
    QualifiedIdentifier name;
    NESValue value;

public:
    NamedNESValue(QualifiedIdentifier name, NESValue value);

    [[nodiscard]] const QualifiedIdentifier& getFullyQualifiedName() const;
    [[nodiscard]] const NESValue& getValue() const;
    [[nodiscard]] bool operator==(const NamedNESValue& other) const;
    friend std::ostream& operator<<(std::ostream& os, const NamedNESValue& obj);
};

/// Positional or named value arguments passed to `NESType::construct(...)`.
class NESValueParameters
{
public:
    using Storage = std::variant<std::vector<NESValue>, Schema<NamedNESValue, Ordered>>;

private:
    Storage storage;

public:
    NESValueParameters();
    explicit NESValueParameters(std::vector<NESValue> positionalValues);
    explicit NESValueParameters(Schema<NamedNESValue, Ordered> namedValues);

    static NESValueParameters positional(std::vector<NESValue> values);
    static NESValueParameters named(Schema<NamedNESValue, Ordered> values);

    [[nodiscard]] bool isNamed() const noexcept;
    [[nodiscard]] size_t size() const noexcept;
    [[nodiscard]] bool empty() const noexcept;

    [[nodiscard]] std::optional<NESValue> at(size_t index) const;
    [[nodiscard]] std::optional<NESValue> getByName(const QualifiedIdentifier& name) const;
    [[nodiscard]] const Schema<NamedNESValue, Ordered>* tryGetNamedSchema() const noexcept;
    [[nodiscard]] const std::vector<NESValue>* tryGetPositional() const noexcept;

    /// Binds positional or named arguments against an ordered formal parameter list.
    [[nodiscard]] std::expected<Schema<NamedNESValue, Ordered>, Exception>
    bindAgainst(std::span<const QualifiedIdentifier> formalNames) const;
};

class NamedNESTypeParameter
{
    std::variant<NESType, NESValue> value;
    QualifiedIdentifier name;

public:
    NamedNESTypeParameter(QualifiedIdentifier name, NESType type);
    NamedNESTypeParameter(NESType type, QualifiedIdentifier name);
    NamedNESTypeParameter(QualifiedIdentifier name, NESValue val);
    NamedNESTypeParameter(NESValue val, QualifiedIdentifier name);
    NamedNESTypeParameter(QualifiedIdentifier name, std::variant<NESType, NESValue> param);

    [[nodiscard]] const QualifiedIdentifier& getFullyQualifiedName() const;
    [[nodiscard]] bool isType() const noexcept;
    [[nodiscard]] bool isValue() const noexcept;
    [[nodiscard]] const NESType* tryGetType() const noexcept;
    [[nodiscard]] const NESValue* tryGetValue() const noexcept;
    [[nodiscard]] const std::variant<NESType, NESValue>& getUnderlying() const noexcept;
    [[nodiscard]] bool operator==(const NamedNESTypeParameter& other) const;
    friend std::ostream& operator<<(std::ostream& os, const NamedNESTypeParameter& obj);
};

/// Positional or named type/non-type arguments passed to a `NESFunctor` (`Array<INT32, 5>`, `INT32(nullable=false)`, etc.).
class NESTypeParameters
{
public:
    using Element = std::variant<NESType, NESValue>;
    using Storage = std::variant<std::vector<Element>, Schema<NamedNESTypeParameter, Ordered>>;

private:
    Storage storage;

public:
    NESTypeParameters();
    explicit NESTypeParameters(std::vector<Element> positionalParams);
    explicit NESTypeParameters(Schema<NamedNESTypeParameter, Ordered> namedParams);

    static NESTypeParameters fromTypes(std::vector<NESType> types);
    static NESTypeParameters fromValues(std::vector<NESValue> values);
    static NESTypeParameters named(Schema<NamedNESTypeParameter, Ordered> params);

    [[nodiscard]] bool isNamed() const noexcept;
    [[nodiscard]] size_t size() const noexcept;
    [[nodiscard]] bool empty() const noexcept;

    [[nodiscard]] std::optional<Element> at(size_t index) const;
    [[nodiscard]] std::optional<NESType> getTypeAt(size_t index) const;
    [[nodiscard]] std::optional<NESValue> getValueAt(size_t index) const;
    [[nodiscard]] std::optional<NamedNESTypeParameter> getByName(const QualifiedIdentifier& name) const;
    [[nodiscard]] std::optional<NESType> getTypeByName(const QualifiedIdentifier& name) const;
    [[nodiscard]] std::optional<NESValue> getValueByName(const QualifiedIdentifier& name) const;

    [[nodiscard]] std::expected<Schema<NamedNESTypeParameter, Ordered>, Exception>
    bindAgainst(std::span<const QualifiedIdentifier> formalNames) const;

    [[nodiscard]] bool operator==(const NESTypeParameters& other) const;
};

using PassedNESTypeParameters = NESTypeParameters;

/// A predicate over `NESType`s (corresponding to a C++ `concept` or Haskell/Rust type class).
/// Supports both trait-backed open polymorphism (`NESTypeClass::forTrait<MLModelTrait>()`) and
/// structural predicates (`ClosedBinaryFunction("Add")`).
class NESTypeClass
{
    ExplicitAny typeClassDescriptor;
    std::function<bool(const ExplicitAny&, const NESType&)> predicate;
    std::function<bool(const ExplicitAny&, const ExplicitAny&)> typeClassEquals;

    NESTypeClass(
        ExplicitAny typeClassDescriptor,
        std::function<bool(const ExplicitAny&, const NESType&)> predicate,
        std::function<bool(const ExplicitAny&, const ExplicitAny&)> typeClassEquals);

public:
    template <typename Descriptor>
    NESTypeClass(
        Descriptor descriptor,
        std::type_identity_t<std::function<bool(const Descriptor&, const NESType&)>> predicate,
        std::type_identity_t<std::function<bool(const Descriptor&, const Descriptor&)>> typeClassEquals =
            [](const Descriptor& lhs, const Descriptor& rhs)
        {
            if constexpr (std::equality_comparable<Descriptor>)
            {
                return lhs == rhs;
            }
            else
            {
                return &lhs == &rhs;
            }
        })
        : typeClassDescriptor(ExplicitAny::create(std::move(descriptor)))
        , predicate(
              [predicate = std::move(predicate)](const ExplicitAny& descAny, const NESType& type)
              {
                  if (const auto* casted = descAny.tryGetAs<Descriptor>())
                  {
                      return predicate(*casted, type);
                  }
                  return false;
              })
        , typeClassEquals(
              [typeClassEquals = std::move(typeClassEquals)](const ExplicitAny& self, const ExplicitAny& other)
              {
                  const auto* castedSelf = self.tryGetAs<Descriptor>();
                  const auto* castedOther = other.tryGetAs<Descriptor>();
                  if (castedSelf != nullptr && castedOther != nullptr)
                  {
                      return typeClassEquals(*castedSelf, *castedOther);
                  }
                  return false;
              })
    {
    }

    static NESTypeClass any();

    template <typename Trait>
    static NESTypeClass forTrait()
    {
        struct TraitTag
        {
            std::type_index index{typeid(std::remove_cvref_t<Trait>)};
            bool operator==(const TraitTag&) const = default;
        };
        return NESTypeClass{TraitTag{}, [](const TraitTag&, const NESType& type) { return type.hasTrait<Trait>(); }};
    }

    [[nodiscard]] bool validate(const NESType& type) const;
    [[nodiscard]] bool operator==(const NESTypeClass& other) const;

    friend NESTypeClass operator&&(const NESTypeClass& lhs, const NESTypeClass& rhs);
    friend NESTypeClass operator||(const NESTypeClass& lhs, const NESTypeClass& rhs);
};

class NamedNESTypeClass
{
    QualifiedIdentifier name;
    NESTypeClass typeClass;

public:
    NamedNESTypeClass(QualifiedIdentifier name, NESTypeClass typeClass);

    [[nodiscard]] const QualifiedIdentifier& getFullyQualifiedName() const;
    [[nodiscard]] const NESTypeClass& getTypeClass() const;
    [[nodiscard]] bool validate(const NESType& type) const;
    [[nodiscard]] bool operator==(const NamedNESTypeClass& other) const;
    friend std::ostream& operator<<(std::ostream& os, const NamedNESTypeClass& obj);
};

class NESTypeClassParameters
{
    NESTypeParameters parameters;

public:
    NESTypeClassParameters() = default;
    explicit NESTypeClassParameters(NESTypeParameters parameters);

    [[nodiscard]] const NESTypeParameters& getParameters() const noexcept;
    [[nodiscard]] bool empty() const noexcept;
    [[nodiscard]] size_t size() const noexcept;
    [[nodiscard]] bool operator==(const NESTypeClassParameters& other) const = default;
};

/// A parameterized constructor for a `NESTypeClass` (corresponds to a templated `concept` in C++).
/// When a `NESParametrizedTypeClass` takes 0 parameters, `unparametrized()` yields its unique `NESTypeClass`.
class NESParametrizedTypeClass
{
    ExplicitAny structure;
    std::function<bool(const ExplicitAny&, const ExplicitAny&)> structureEquals;
    std::function<std::expected<NESTypeClass, Exception>(const ExplicitAny&, const NESTypeClassParameters&)> typeClassConstructor;

public:
    template <typename Descriptor>
    NESParametrizedTypeClass(
        Descriptor descriptor,
        std::type_identity_t<std::function<std::expected<NESTypeClass, Exception>(const Descriptor&, const NESTypeClassParameters&)>>
            constructor,
        std::type_identity_t<std::function<bool(const Descriptor&, const Descriptor&)>> equals =
            [](const Descriptor& lhs, const Descriptor& rhs)
        {
            if constexpr (std::equality_comparable<Descriptor>)
            {
                return lhs == rhs;
            }
            else
            {
                return &lhs == &rhs;
            }
        })
        : structure(ExplicitAny::create(std::move(descriptor)))
        , structureEquals(
              [equals = std::move(equals)](const ExplicitAny& lhs, const ExplicitAny& rhs)
              {
                  const auto* lhsCasted = lhs.tryGetAs<Descriptor>();
                  const auto* rhsCasted = rhs.tryGetAs<Descriptor>();
                  if (lhsCasted != nullptr && rhsCasted != nullptr)
                  {
                      return equals(*lhsCasted, *rhsCasted);
                  }
                  return false;
              })
        , typeClassConstructor(
              [constructor = std::move(constructor)](const ExplicitAny& descAny, const NESTypeClassParameters& params)
              {
                  const auto* casted = descAny.tryGetAs<Descriptor>();
                  PRECONDITION(casted != nullptr, "NESParametrizedTypeClass descriptor mismatch");
                  return constructor(*casted, params);
              })
    {
    }

    static NESParametrizedTypeClass constant(NESTypeClass constantTypeClass);

    [[nodiscard]] std::expected<NESTypeClass, Exception> instantiate(const NESTypeClassParameters& params = {}) const;
    [[nodiscard]] NESTypeClass unparametrized() const;
    [[nodiscard]] bool operator==(const NESParametrizedTypeClass& other) const;
};

class NamedNESParametrizedTypeClass
{
    QualifiedIdentifier name;
    NESParametrizedTypeClass underlying;

public:
    NamedNESParametrizedTypeClass(QualifiedIdentifier name, NESParametrizedTypeClass underlying);

    static NamedNESParametrizedTypeClass constant(QualifiedIdentifier name, NESTypeClass constantTypeClass);

    [[nodiscard]] const QualifiedIdentifier& getFullyQualifiedName() const;
    [[nodiscard]] const NESParametrizedTypeClass& getUnderlying() const;
    [[nodiscard]] std::expected<NESTypeClass, Exception> instantiate(const NESTypeClassParameters& params = {}) const;
    [[nodiscard]] NESTypeClass unparametrized() const;
    [[nodiscard]] bool operator==(const NamedNESParametrizedTypeClass& other) const;
    friend std::ostream& operator<<(std::ostream& os, const NamedNESParametrizedTypeClass& obj);
};

/// Formal parameter specification for a `NESFunctor` (supporting both type parameters `typename T` and
/// non-type value parameters `Value V`, each constrained by a `NESTypeClass`).
struct ExpectedNESTypeParameter
{
    enum class Kind : uint8_t
    {
        TypeParameter,
        ValueParameter,
    };

    QualifiedIdentifier name;
    NESTypeClass constraint;
    Kind kind = Kind::TypeParameter;
    bool optional = false;

    [[nodiscard]] const QualifiedIdentifier& getFullyQualifiedName() const { return name; }
    [[nodiscard]] bool operator==(const ExpectedNESTypeParameter& other) const = default;
    friend std::ostream& operator<<(std::ostream& os, const ExpectedNESTypeParameter& obj);
};

class ExpectedNESTypeArguments
{
    Schema<ExpectedNESTypeParameter, Ordered> expectedParameters;

public:
    ExpectedNESTypeArguments() = default;
    explicit ExpectedNESTypeArguments(Schema<ExpectedNESTypeParameter, Ordered> expectedParameters);
    explicit ExpectedNESTypeArguments(const Schema<NamedNESTypeClass, Ordered>& expectedTypeClasses);

    [[nodiscard]] const Schema<ExpectedNESTypeParameter, Ordered>& getParameters() const noexcept;
    [[nodiscard]] size_t size() const noexcept;
    [[nodiscard]] bool empty() const noexcept;

    /// Validates `passed` against parameter kinds & `NESTypeClass` predicates, returning named bound parameters.
    [[nodiscard]] std::expected<NESTypeParameters, Exception> validateAndBind(const NESTypeParameters& passed) const;
};

/// A type constructor (possibly nullary).
/// Example: `FixedSizeArray` taking `(ELEMENT_TYPE: ScalarDatatype, SIZE: IntegerValue)`, or nullary `OPENVINO_MODEL`.
/// When registered via a `RuntimeRegistry`, `structure` holds `Registry::EntryType` so existing C++ registries
/// continue to receive their typed entries seamlessly alongside `NESTypeCatalog`.
class NESFunctor
{
    ExplicitAny structure;
    std::function<bool(const ExplicitAny&, const ExplicitAny&)> typeStructureEquals;
    std::optional<ExpectedNESTypeArguments> expectedArguments;
    std::function<std::expected<NESType, Exception>(const ExplicitAny&, const NESTypeParameters&)> typeConstructor;

public:
    template <typename Descriptor>
    NESFunctor(
        Descriptor descriptor,
        std::type_identity_t<std::function<bool(const Descriptor&, const Descriptor&)>> structureEquals,
        std::type_identity_t<std::function<std::expected<NESType, Exception>(const Descriptor&, const NESTypeParameters&)>> constructor,
        std::optional<ExpectedNESTypeArguments> expectedArgs = std::nullopt)
        : structure(ExplicitAny::create(std::move(descriptor)))
        , typeStructureEquals(
              [structureEquals = std::move(structureEquals)](const ExplicitAny& self, const ExplicitAny& other)
              {
                  const auto* selfCasted = self.tryGetAs<Descriptor>();
                  const auto* otherCasted = other.tryGetAs<Descriptor>();
                  if (selfCasted != nullptr && otherCasted != nullptr)
                  {
                      return structureEquals(*selfCasted, *otherCasted);
                  }
                  return false;
              })
        , expectedArguments(std::move(expectedArgs))
        , typeConstructor(
              [constructor = std::move(constructor)](const ExplicitAny& self, const NESTypeParameters& params)
              {
                  const auto* casted = self.tryGetAs<Descriptor>();
                  PRECONDITION(casted != nullptr, "NESFunctor structure type mismatch during instantiation");
                  return constructor(*casted, params);
              })
    {
    }

    template <typename Descriptor>
    static NESFunctor nullary(
        Descriptor descriptor,
        std::type_identity_t<std::function<std::expected<NESType, Exception>(const Descriptor&)>> factory,
        std::type_identity_t<std::function<bool(const Descriptor&, const Descriptor&)>> structureEquals =
            [](const Descriptor& lhs, const Descriptor& rhs)
        {
            if constexpr (std::equality_comparable<Descriptor>)
            {
                return lhs == rhs;
            }
            else
            {
                return &lhs == &rhs;
            }
        })
    {
        return NESFunctor{
            std::move(descriptor),
            std::move(structureEquals),
            [factory = std::move(factory)](const Descriptor& desc, const NESTypeParameters& params) -> std::expected<NESType, Exception>
            {
                if (!params.empty())
                {
                    return std::unexpected{
                        TypeInferenceException("Nullary functor expects 0 type arguments, but got {}", params.size())};
                }
                return factory(desc);
            },
            ExpectedNESTypeArguments{}};
    }

    [[nodiscard]] std::expected<NESType, Exception> instantiate(const NESTypeParameters& params = {}) const;
    [[nodiscard]] const std::optional<ExpectedNESTypeArguments>& getExpectedArguments() const noexcept;
    [[nodiscard]] const ExplicitAny& getStructure() const noexcept;

    template <typename T>
    [[nodiscard]] const std::remove_cvref_t<T>* tryGetStructureAs() const noexcept
    {
        return structure.tryGetAs<T>();
    }

    [[nodiscard]] bool operator==(const NESFunctor& other) const;
};

class NamedNESFunctor
{
    QualifiedIdentifier name;
    NESFunctor underlyingFunctor;

public:
    NamedNESFunctor(QualifiedIdentifier name, NESFunctor underlyingFunctor);

    [[nodiscard]] const QualifiedIdentifier& getFullyQualifiedName() const;
    [[nodiscard]] const NESFunctor& getUnderlyingFunctor() const;
    [[nodiscard]] std::expected<NESType, Exception> instantiate(const NESTypeParameters& params = {}) const;
    [[nodiscard]] bool operator==(const NamedNESFunctor& other) const;
    friend std::ostream& operator<<(std::ostream& os, const NamedNESFunctor& obj);
};

}

/// NOLINTBEGIN(cert-dcl58-cpp)
template <>
struct std::hash<NES::NamedNESType>
{
    size_t operator()(const NES::NamedNESType& val) const noexcept
    {
        return std::hash<NES::QualifiedIdentifier>{}(val.getFullyQualifiedName());
    }
};

template <>
struct std::hash<NES::NamedNESValue>
{
    size_t operator()(const NES::NamedNESValue& val) const noexcept
    {
        return std::hash<NES::QualifiedIdentifier>{}(val.getFullyQualifiedName());
    }
};

template <>
struct std::hash<NES::NamedNESTypeParameter>
{
    size_t operator()(const NES::NamedNESTypeParameter& val) const noexcept
    {
        return std::hash<NES::QualifiedIdentifier>{}(val.getFullyQualifiedName());
    }
};

template <>
struct std::hash<NES::NamedNESTypeClass>
{
    size_t operator()(const NES::NamedNESTypeClass& val) const noexcept
    {
        return std::hash<NES::QualifiedIdentifier>{}(val.getFullyQualifiedName());
    }
};

template <>
struct std::hash<NES::NamedNESParametrizedTypeClass>
{
    size_t operator()(const NES::NamedNESParametrizedTypeClass& val) const noexcept
    {
        return std::hash<NES::QualifiedIdentifier>{}(val.getFullyQualifiedName());
    }
};

template <>
struct std::hash<NES::ExpectedNESTypeParameter>
{
    size_t operator()(const NES::ExpectedNESTypeParameter& val) const noexcept
    {
        return std::hash<NES::QualifiedIdentifier>{}(val.getFullyQualifiedName());
    }
};

template <>
struct std::hash<NES::NamedNESFunctor>
{
    size_t operator()(const NES::NamedNESFunctor& val) const noexcept
    {
        return std::hash<NES::QualifiedIdentifier>{}(val.getFullyQualifiedName());
    }
};
/// NOLINTEND(cert-dcl58-cpp)

FMT_OSTREAM(NES::NamedNESType);
FMT_OSTREAM(NES::NamedNESValue);
FMT_OSTREAM(NES::NamedNESTypeParameter);
FMT_OSTREAM(NES::NamedNESTypeClass);
FMT_OSTREAM(NES::NamedNESParametrizedTypeClass);
FMT_OSTREAM(NES::ExpectedNESTypeParameter);
FMT_OSTREAM(NES::NamedNESFunctor);
