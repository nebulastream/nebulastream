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

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <functional>
#include <memory>
#include <optional>
#include <ostream>
#include <span>
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
class NESParameterId;
template <typename T>
class NESValueParameter;
class QualifiedErasedNESValueParameter;
class ExpectedNESValueArguments;
class InstantiatedNESValueParameter;
class InstantiatedNESValueParameters;
class NamedNESType;
class NamedNESTypeParameter;
class NESTypeParameters;
class NESTypeClass;
class NamedNESTypeClass;
class NESTypeClassParameters;
class NESParametrizedTypeClass;
class NamedNESParametrizedTypeClass;
class NESTypeParameter;
class NESOptionalTypeParameter;
class NESFunctorParameter;
class NESOptionalFunctorParameter;
template <typename T>
class NESNonTypeParameter;
class QualifiedErasedNESTypeParameter;
class ExpectedNESTypeArguments;
class InstantiatedNESTypeParameter;
class InstantiatedNESTypeParameters;
class NESFunctor;
class NamedNESFunctor;

namespace detail
{
using TypeTraitMap = std::unordered_map<std::type_index, ExplicitAny>;

template <typename T>
struct IsExpectedValue : std::false_type
{
};

template <typename V>
struct IsExpectedValue<std::expected<V, Exception>> : std::true_type
{
    using ValueType = V;
};

template <typename T>
concept RawConstructibleNESTypeImpl = requires(const T& self, const NESValueParameters& params) {
    requires IsExpectedValue<decltype(self.construct(params))>::value;
};

template <typename T>
concept SchemaConstructibleNESTypeImpl = requires(const T& self, const InstantiatedNESValueParameters& params) {
    { self.getExpectedValueArguments() } -> std::convertible_to<ExpectedNESValueArguments>;
    requires IsExpectedValue<decltype(self.construct(params))>::value;
};

template <typename T>
concept ConstructibleNESTypeImpl = RawConstructibleNESTypeImpl<T> || SchemaConstructibleNESTypeImpl<T>;

template <typename T>
concept HasExplicitNESValueType = requires { typename T::ValueType; };

template <typename T>
concept HasCustomEqualsValue = requires(const T& self, const ExplicitAny& lhs, const ExplicitAny& rhs) {
    { self.equalsValue(lhs, rhs) } -> std::convertible_to<bool>;
};

template <
    typename T,
    bool IsRawConstructible = RawConstructibleNESTypeImpl<T>,
    bool IsSchemaConstructible = SchemaConstructibleNESTypeImpl<T>,
    bool HasAlias = HasExplicitNESValueType<T>>
struct DeducedNESValueRepr
{
    using Type = T;
};

template <typename T, bool IsSchemaConstructible, bool HasAlias>
struct DeducedNESValueRepr<T, true, IsSchemaConstructible, HasAlias>
{
    using Type = typename IsExpectedValue<decltype(std::declval<const T&>().construct(std::declval<const NESValueParameters&>()))>::ValueType;
};

template <typename T, bool HasAlias>
struct DeducedNESValueRepr<T, false, true, HasAlias>
{
    using Type =
        typename IsExpectedValue<decltype(std::declval<const T&>().construct(std::declval<const InstantiatedNESValueParameters&>()))>::ValueType;
};

template <typename T>
struct DeducedNESValueRepr<T, false, false, true>
{
    using Type = typename T::ValueType;
};

struct ErasedNESType
{
    virtual ~ErasedNESType() = default;
    [[nodiscard]] virtual std::expected<ExplicitAny, Exception> constructRaw(const NESValueParameters& params) const = 0;
    [[nodiscard]] virtual bool equals(const ErasedNESType& other) const = 0;
    [[nodiscard]] virtual bool equalsValue(const ExplicitAny& lhs, const ExplicitAny& rhs) const = 0;
};

template <typename T>
struct NESTypeModel;

struct NESValueImpl;
}

/// Concept fulfilled by concrete C++ types wrapped in `NESType` (analogous to `LogicalOperatorConcept` / `LogicalFunctionConcept`).
/// A concrete type implements `operator==`, and optionally:
/// - `construct(const NESValueParameters&) const -> std::expected<ValueRepr, Exception>` for value-constructible types, or
/// - `using ValueType = ValueRepr;` for unconstructible types whose values are created via `NESValue::create(...)`.
template <typename T>
concept NESTypeConcept
    = (!std::same_as<std::remove_cvref_t<T>, NESType>) && std::equality_comparable<std::remove_cvref_t<T>>;

/// A fully instantiated type (comparable to a fully instantiated template in C++).
/// For example, `FixedSizeArray<VARCHAR, 5>`, `DataType(INT32, NOT_NULLABLE)`, or `OpenVinoModel`.
/// Implemented as an immutable flyweight wrapper around `detail::ErasedNESType` plus an open `TypeTraitMap`.
class NESType
{
    std::shared_ptr<const detail::ErasedNESType> self;
    std::shared_ptr<const detail::TypeTraitMap> typeTraits;

    NESType(
        std::shared_ptr<const detail::ErasedNESType> self,
        std::shared_ptr<const detail::TypeTraitMap> typeTraits);

public:
    template <NESTypeConcept T>
    explicit NESType(T typeImpl);

    template <typename TypeTrait>
    [[nodiscard]] NESType withTypeTrait(TypeTrait typeTrait) const
    {
        using CleanTypeTrait = std::remove_cvref_t<TypeTrait>;
        auto updatedTraits = typeTraits != nullptr ? std::make_shared<detail::TypeTraitMap>(*typeTraits)
                                                   : std::make_shared<detail::TypeTraitMap>();
        updatedTraits->insert_or_assign(
            std::type_index{typeid(CleanTypeTrait)}, ExplicitAny::create(CleanTypeTrait{std::move(typeTrait)}));
        return NESType{self, std::move(updatedTraits)};
    }

    template <typename TypeTrait>
    [[nodiscard]] const std::remove_cvref_t<TypeTrait>* tryGetTypeTrait() const noexcept
    {
        using CleanTypeTrait = std::remove_cvref_t<TypeTrait>;
        if (typeTraits == nullptr)
        {
            return nullptr;
        }
        if (const auto it = typeTraits->find(std::type_index{typeid(CleanTypeTrait)}); it != typeTraits->end())
        {
            return it->second.tryGetAs<CleanTypeTrait>();
        }
        return nullptr;
    }

    template <typename TypeTrait>
    [[nodiscard]] bool hasTypeTrait() const noexcept
    {
        return tryGetTypeTrait<TypeTrait>() != nullptr;
    }

    template <typename T>
    [[nodiscard]] const std::remove_cvref_t<T>* tryGetAs() const noexcept;

    [[nodiscard]] std::expected<NESValue, Exception> construct(const NESValueParameters& params) const;
    [[nodiscard]] std::expected<NESValue, Exception> construct() const;

    [[nodiscard]] bool operator==(const NESType& other) const;
    [[nodiscard]] bool equalsValue(const ExplicitAny& lhs, const ExplicitAny& rhs) const;
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
/// Implemented as an immutable flyweight (`std::shared_ptr<const detail::NESValueImpl>`) so copies are O(1) and satisfy `copy == orig`.
class NESValue
{
    std::shared_ptr<const detail::NESValueImpl> impl;

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
        return getInternalRepresentation().tryGetAs<T>();
    }

    template <typename T>
    [[nodiscard]] T getAs() const
    {
        return getInternalRepresentation().getAs<T>();
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
};

namespace detail
{
struct ErasedNESFunctor
{
    virtual ~ErasedNESFunctor() = default;
    [[nodiscard]] virtual std::expected<NESType, Exception> instantiateRaw(const NESTypeParameters& params) const = 0;
    [[nodiscard]] virtual const std::optional<ExpectedNESTypeArguments>& getExpectedArguments() const noexcept = 0;
    [[nodiscard]] virtual const ExplicitAny* tryGetRegistryEntry() const noexcept = 0;
    [[nodiscard]] virtual bool equals(const ErasedNESFunctor& other) const = 0;
};

template <typename T>
concept RawFunctorImpl = requires(const std::remove_cvref_t<T>& self, const NESTypeParameters& params) {
    { self.instantiate(params) } -> std::same_as<std::expected<NESType, Exception>>;
};

template <typename T>
concept SchemaFunctorImpl = requires(const std::remove_cvref_t<T>& self, const InstantiatedNESTypeParameters& params) {
    self.getExpectedArguments();
    { self.instantiate(params) } -> std::same_as<std::expected<NESType, Exception>>;
};
}

template <typename T>
concept NESFunctorConcept = (!std::same_as<std::remove_cvref_t<T>, NESFunctor>)
    && std::equality_comparable<std::remove_cvref_t<T>>
    && (detail::RawFunctorImpl<T> || detail::SchemaFunctorImpl<T>);

/// A type constructor (possibly nullary).
/// Example: `FixedSizeArray` taking `(ELEMENT_TYPE: ScalarDatatype, SIZE: IntegerDatatype)`, or nullary `OPENVINO_MODEL`.
class NESFunctor
{
    std::shared_ptr<const detail::ErasedNESFunctor> self;

public:
    template <NESFunctorConcept T>
    explicit NESFunctor(T functorImpl);

    [[nodiscard]] std::expected<NESType, Exception> instantiate(const NESTypeParameters& params) const;
    [[nodiscard]] std::expected<NESType, Exception> instantiate() const;
    [[nodiscard]] const std::optional<ExpectedNESTypeArguments>& getExpectedArguments() const noexcept;

    template <typename T>
    [[nodiscard]] const std::remove_cvref_t<T>* tryGetAs() const noexcept;

    template <typename EntryT>
    [[nodiscard]] const std::remove_cvref_t<EntryT>* tryGetRegistryEntryAs() const noexcept;

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
    [[nodiscard]] std::expected<NESType, Exception> instantiate(const NESTypeParameters& params) const;
    [[nodiscard]] std::expected<NESType, Exception> instantiate() const;
    [[nodiscard]] bool operator==(const NamedNESFunctor& other) const;
    friend std::ostream& operator<<(std::ostream& os, const NamedNESFunctor& obj);
};

class NamedNESTypeParameter
{
public:
    using Element = std::variant<NESType, NESValue, NESFunctor>;

private:
    Element value;
    QualifiedIdentifier name;

public:
    NamedNESTypeParameter(QualifiedIdentifier name, NESType type);
    NamedNESTypeParameter(NESType type, QualifiedIdentifier name);
    NamedNESTypeParameter(QualifiedIdentifier name, NESValue val);
    NamedNESTypeParameter(NESValue val, QualifiedIdentifier name);
    NamedNESTypeParameter(QualifiedIdentifier name, NESFunctor functor);
    NamedNESTypeParameter(NESFunctor functor, QualifiedIdentifier name);
    NamedNESTypeParameter(QualifiedIdentifier name, Element param);

    [[nodiscard]] const QualifiedIdentifier& getFullyQualifiedName() const;
    [[nodiscard]] bool isType() const noexcept;
    [[nodiscard]] bool isValue() const noexcept;
    [[nodiscard]] bool isFunctor() const noexcept;
    [[nodiscard]] const NESType* tryGetType() const noexcept;
    [[nodiscard]] const NESValue* tryGetValue() const noexcept;
    [[nodiscard]] const NESFunctor* tryGetFunctor() const noexcept;
    [[nodiscard]] const Element& getUnderlying() const noexcept;
    [[nodiscard]] bool operator==(const NamedNESTypeParameter& other) const;
    friend std::ostream& operator<<(std::ostream& os, const NamedNESTypeParameter& obj);
};

/// Positional or named type/non-type/higher-kinded functor arguments passed to a `NESFunctor` or `NESParametrizedTypeClass`.
class NESTypeParameters
{
public:
    using Element = NamedNESTypeParameter::Element;
    using Storage = std::variant<std::vector<Element>, Schema<NamedNESTypeParameter, Ordered>>;

private:
    Storage storage;

public:
    NESTypeParameters();
    explicit NESTypeParameters(std::vector<Element> positionalParams);
    explicit NESTypeParameters(Schema<NamedNESTypeParameter, Ordered> namedParams);

    static NESTypeParameters fromTypes(std::vector<NESType> types);
    static NESTypeParameters fromValues(std::vector<NESValue> values);
    static NESTypeParameters fromFunctors(std::vector<NESFunctor> functors);
    static NESTypeParameters named(Schema<NamedNESTypeParameter, Ordered> params);

    [[nodiscard]] bool isNamed() const noexcept;
    [[nodiscard]] size_t size() const noexcept;
    [[nodiscard]] bool empty() const noexcept;

    [[nodiscard]] std::optional<Element> at(size_t index) const;
    [[nodiscard]] std::optional<NESType> getTypeAt(size_t index) const;
    [[nodiscard]] std::optional<NESValue> getValueAt(size_t index) const;
    [[nodiscard]] std::optional<NESFunctor> getFunctorAt(size_t index) const;
    [[nodiscard]] std::optional<NamedNESTypeParameter> getByName(const QualifiedIdentifier& name) const;
    [[nodiscard]] std::optional<NESType> getTypeByName(const QualifiedIdentifier& name) const;
    [[nodiscard]] std::optional<NESValue> getValueByName(const QualifiedIdentifier& name) const;
    [[nodiscard]] std::optional<NESFunctor> getFunctorByName(const QualifiedIdentifier& name) const;
    [[nodiscard]] const Schema<NamedNESTypeParameter, Ordered>* tryGetNamedSchema() const noexcept;
    [[nodiscard]] const std::vector<Element>* tryGetPositional() const noexcept;

    [[nodiscard]] bool operator==(const NESTypeParameters& other) const;
};

using PassedNESTypeParameters = NESTypeParameters;

/// Concept fulfilled by concrete C++ predicates wrapped in `NESTypeClass`.
template <typename T>
concept NESTypeClassConcept = (!std::same_as<std::remove_cvref_t<T>, NESTypeClass>)
    && std::equality_comparable<std::remove_cvref_t<T>>
    && requires(const std::remove_cvref_t<T>& self, const NESType& type) {
           { self.validate(type) } -> std::convertible_to<bool>;
       };

namespace detail
{
struct ErasedNESTypeClass
{
    virtual ~ErasedNESTypeClass() = default;
    [[nodiscard]] virtual bool validate(const NESType& type) const = 0;
    [[nodiscard]] virtual bool equals(const ErasedNESTypeClass& other) const = 0;
};

template <NESTypeClassConcept T>
struct NESTypeClassModel final : ErasedNESTypeClass
{
    T impl;

    explicit NESTypeClassModel(T impl) : impl(std::move(impl)) { }

    [[nodiscard]] bool validate(const NESType& type) const override { return impl.validate(type); }

    [[nodiscard]] bool equals(const ErasedNESTypeClass& other) const override
    {
        if (const auto* otherModel = dynamic_cast<const NESTypeClassModel<T>*>(&other))
        {
            return impl == otherModel->impl;
        }
        return false;
    }
};

template <typename TypeTrait>
struct TypeTraitBackedTypeClass
{
    bool operator==(const TypeTraitBackedTypeClass&) const = default;
    [[nodiscard]] bool validate(const NESType& type) const { return type.hasTypeTrait<TypeTrait>(); }
};
}

/// A predicate over `NESType`s (corresponding to a C++ `concept` or Haskell/Rust type class).
/// Supports both `TypeTrait`-backed open polymorphism (`NESTypeClass::forTypeTrait<MLModelTypeTrait>()`) and
/// structural predicates (`ClosedBinaryFunction(Add)`).
class NESTypeClass
{
    std::shared_ptr<const detail::ErasedNESTypeClass> self;

public:
    template <NESTypeClassConcept T>
    explicit NESTypeClass(T typeClassImpl)
        : self(std::make_shared<detail::NESTypeClassModel<std::remove_cvref_t<T>>>(std::move(typeClassImpl)))
    {
    }

    static NESTypeClass any();

    template <typename TypeTrait>
    static NESTypeClass forTypeTrait()
    {
        return NESTypeClass{detail::TypeTraitBackedTypeClass<std::remove_cvref_t<TypeTrait>>{}};
    }

    template <typename T>
    [[nodiscard]] const std::remove_cvref_t<T>* tryGetAs() const noexcept
    {
        using CleanT = std::remove_cvref_t<T>;
        if constexpr (NESTypeClassConcept<CleanT>)
        {
            if (const auto* model = dynamic_cast<const detail::NESTypeClassModel<CleanT>*>(self.get()))
            {
                return &model->impl;
            }
        }
        return nullptr;
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

/// Address-based identity of a declared `NESTypeParameter`, `NESOptionalTypeParameter`, `NESFunctorParameter`,
/// `NESOptionalFunctorParameter`, `NESNonTypeParameter<T>`, or `NESValueParameter<T>` instance (mirroring `ConfigFieldId` in
/// `nes-configurations`). Parameter declarations are non-copyable and non-movable so each declaration's address uniquely identifies it.
class NESParameterId
{
    uintptr_t parameterAddress;

public:
    explicit NESParameterId(const NESTypeParameter& param) noexcept;
    explicit NESParameterId(const NESOptionalTypeParameter& param) noexcept;
    explicit NESParameterId(const NESFunctorParameter& param) noexcept;
    explicit NESParameterId(const NESOptionalFunctorParameter& param) noexcept;

    template <typename T>
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - a parameter instance's identity is its address
    explicit NESParameterId(const NESNonTypeParameter<T>& param) noexcept
        : parameterAddress(reinterpret_cast<uintptr_t>(std::addressof(param)))
    {
    }

    template <typename T>
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - a parameter instance's identity is its address
    explicit NESParameterId(const NESValueParameter<T>& param) noexcept
        : parameterAddress(reinterpret_cast<uintptr_t>(std::addressof(param)))
    {
    }

    [[nodiscard]] uintptr_t rawAddress() const noexcept { return parameterAddress; }
    friend bool operator==(const NESParameterId& lhs, const NESParameterId& rhs) = default;
    friend std::ostream& operator<<(std::ostream& os, const NESParameterId& id);
};

namespace detail
{
struct NESParameterIdHash
{
    size_t operator()(const NESParameterId& id) const noexcept { return std::hash<uintptr_t>{}(id.rawAddress()); }
};
}

/// Declares a single formal type parameter (`typename T` constrained by a `NESTypeClass`, optionally with a default `NESType`).
/// Declared once per plugin/functor and passed directly to `InstantiatedNESTypeParameters::get(PARAM)`.
class NESTypeParameter
{
    Identifier name;
    std::string description;
    std::function<NESTypeClass()> constraintSupplier;
    std::optional<std::function<NESType()>> defaultSupplier;

public:
    NESTypeParameter(Identifier name, std::string description, NESTypeClass constraint = NESTypeClass::any());
    NESTypeParameter(Identifier name, std::string description, std::function<NESTypeClass()> constraintSupplier);
    NESTypeParameter(Identifier name, std::string description, NESTypeClass constraint, NESType defaultType);
    NESTypeParameter(
        Identifier name,
        std::string description,
        std::function<NESTypeClass()> constraintSupplier,
        std::function<NESType()> defaultSupplier);

    NESTypeParameter(const NESTypeParameter&) = delete;
    NESTypeParameter(NESTypeParameter&&) noexcept = delete;
    NESTypeParameter& operator=(const NESTypeParameter&) = delete;
    NESTypeParameter& operator=(NESTypeParameter&&) noexcept = delete;
    ~NESTypeParameter() = default;

    [[nodiscard]] const Identifier& getName() const noexcept { return name; }
    [[nodiscard]] std::string_view getDescription() const noexcept { return description; }
    [[nodiscard]] const std::function<NESTypeClass()>& getConstraintSupplier() const noexcept { return constraintSupplier; }
    [[nodiscard]] const std::optional<std::function<NESType()>>& getDefaultSupplier() const noexcept { return defaultSupplier; }
};

/// Declares an optional formal type parameter with no default, yielding `std::optional<NESType>` from `params.get(PARAM)`.
class NESOptionalTypeParameter
{
    Identifier name;
    std::string description;
    std::function<NESTypeClass()> constraintSupplier;

public:
    NESOptionalTypeParameter(Identifier name, std::string description, NESTypeClass constraint = NESTypeClass::any());
    NESOptionalTypeParameter(Identifier name, std::string description, std::function<NESTypeClass()> constraintSupplier);

    NESOptionalTypeParameter(const NESOptionalTypeParameter&) = delete;
    NESOptionalTypeParameter(NESOptionalTypeParameter&&) noexcept = delete;
    NESOptionalTypeParameter& operator=(const NESOptionalTypeParameter&) = delete;
    NESOptionalTypeParameter& operator=(NESOptionalTypeParameter&&) noexcept = delete;
    ~NESOptionalTypeParameter() = default;

    [[nodiscard]] const Identifier& getName() const noexcept { return name; }
    [[nodiscard]] std::string_view getDescription() const noexcept { return description; }
    [[nodiscard]] const std::function<NESTypeClass()>& getConstraintSupplier() const noexcept { return constraintSupplier; }
};

/// Declares a higher-kinded formal functor parameter (`template <...> typename F`), allowing functors and parameterized
/// type classes (e.g. `ClosedBinaryFunction(Add)`) to accept unapplied `NESFunctor`s directly via `params.get(PARAM) -> NESFunctor`.
class NESFunctorParameter
{
    Identifier name;
    std::string description;
    std::function<std::expected<NESFunctor, Exception>(const NESFunctor&)> validator;
    std::optional<std::function<NESFunctor()>> defaultSupplier;

public:
    NESFunctorParameter(Identifier name, std::string description);
    NESFunctorParameter(
        Identifier name,
        std::string description,
        std::function<std::expected<NESFunctor, Exception>(const NESFunctor&)> validator);
    NESFunctorParameter(Identifier name, std::string description, NESFunctor defaultFunctor);
    NESFunctorParameter(Identifier name, std::string description, std::function<NESFunctor()> defaultSupplier);

    NESFunctorParameter(const NESFunctorParameter&) = delete;
    NESFunctorParameter(NESFunctorParameter&&) noexcept = delete;
    NESFunctorParameter& operator=(const NESFunctorParameter&) = delete;
    NESFunctorParameter& operator=(NESFunctorParameter&&) noexcept = delete;
    ~NESFunctorParameter() = default;

    [[nodiscard]] const Identifier& getName() const noexcept { return name; }
    [[nodiscard]] std::string_view getDescription() const noexcept { return description; }
    [[nodiscard]] const std::function<std::expected<NESFunctor, Exception>(const NESFunctor&)>& getValidator() const noexcept
    {
        return validator;
    }
    [[nodiscard]] const std::optional<std::function<NESFunctor()>>& getDefaultSupplier() const noexcept { return defaultSupplier; }
};

/// Declares an optional higher-kinded functor parameter with no default, yielding `std::optional<NESFunctor>` from `params.get(PARAM)`.
class NESOptionalFunctorParameter
{
    Identifier name;
    std::string description;
    std::function<std::expected<NESFunctor, Exception>(const NESFunctor&)> validator;

public:
    NESOptionalFunctorParameter(Identifier name, std::string description);
    NESOptionalFunctorParameter(
        Identifier name,
        std::string description,
        std::function<std::expected<NESFunctor, Exception>(const NESFunctor&)> validator);

    NESOptionalFunctorParameter(const NESOptionalFunctorParameter&) = delete;
    NESOptionalFunctorParameter(NESOptionalFunctorParameter&&) noexcept = delete;
    NESOptionalFunctorParameter& operator=(const NESOptionalFunctorParameter&) = delete;
    NESOptionalFunctorParameter& operator=(NESOptionalFunctorParameter&&) noexcept = delete;
    ~NESOptionalFunctorParameter() = default;

    [[nodiscard]] const Identifier& getName() const noexcept { return name; }
    [[nodiscard]] std::string_view getDescription() const noexcept { return description; }
    [[nodiscard]] const std::function<std::expected<NESFunctor, Exception>(const NESFunctor&)>& getValidator() const noexcept
    {
        return validator;
    }
};

namespace detail
{
template <typename T>
std::expected<T, Exception> defaultExtractFromNESValue(const NESValue& val, const Identifier& paramName)
{
    if constexpr (std::same_as<T, NESValue>)
    {
        return val;
    }
    else
    {
        if (const auto* ptr = val.tryGetAs<T>())
        {
            return *ptr;
        }
        return std::unexpected{InvalidQuerySyntax("Invalid value payload passed for parameter '{}'", paramName)};
    }
}
}

/// Declares a single formal non-type value parameter (`T V` constrained by a `NESTypeClass` and lowered via `factory`).
/// Declared once per plugin/functor and passed directly to `InstantiatedNESTypeParameters::get(PARAM) -> T`.
template <typename T = NESValue>
class NESNonTypeParameter
{
    Identifier name;
    std::string description;
    std::function<NESTypeClass()> constraintSupplier;
    std::function<std::expected<T, Exception>(const NESValue&)> factory;
    std::optional<std::function<T()>> defaultSupplier;

public:
    NESNonTypeParameter(Identifier name, std::string description, NESTypeClass constraint = NESTypeClass::any())
        : name(std::move(name))
        , description(std::move(description))
        , constraintSupplier([c = std::move(constraint)] { return c; })
        , factory([paramName = this->name](const NESValue& val) { return detail::defaultExtractFromNESValue<T>(val, paramName); })
        , defaultSupplier(std::nullopt)
    {
    }

    NESNonTypeParameter(Identifier name, std::string description, std::function<NESTypeClass()> constraintSupplier)
        : name(std::move(name))
        , description(std::move(description))
        , constraintSupplier(std::move(constraintSupplier))
        , factory([paramName = this->name](const NESValue& val) { return detail::defaultExtractFromNESValue<T>(val, paramName); })
        , defaultSupplier(std::nullopt)
    {
    }

    NESNonTypeParameter(
        Identifier name,
        std::string description,
        NESTypeClass constraint,
        std::function<std::expected<T, Exception>(const NESValue&)> factory)
        : name(std::move(name))
        , description(std::move(description))
        , constraintSupplier([c = std::move(constraint)] { return c; })
        , factory(std::move(factory))
        , defaultSupplier(std::nullopt)
    {
    }

    NESNonTypeParameter(
        Identifier name,
        std::string description,
        std::function<NESTypeClass()> constraintSupplier,
        std::function<std::expected<T, Exception>(const NESValue&)> factory)
        : name(std::move(name))
        , description(std::move(description))
        , constraintSupplier(std::move(constraintSupplier))
        , factory(std::move(factory))
        , defaultSupplier(std::nullopt)
    {
    }

    NESNonTypeParameter(
        Identifier name,
        std::string description,
        NESTypeClass constraint,
        std::function<std::expected<T, Exception>(const NESValue&)> factory,
        T defaultValue)
        : name(std::move(name))
        , description(std::move(description))
        , constraintSupplier([c = std::move(constraint)] { return c; })
        , factory(std::move(factory))
        , defaultSupplier([defaultValue = std::move(defaultValue)] { return defaultValue; })
    {
    }

    NESNonTypeParameter(const NESNonTypeParameter&) = delete;
    NESNonTypeParameter(NESNonTypeParameter&&) noexcept = delete;
    NESNonTypeParameter& operator=(const NESNonTypeParameter&) = delete;
    NESNonTypeParameter& operator=(NESNonTypeParameter&&) noexcept = delete;
    ~NESNonTypeParameter() = default;

    [[nodiscard]] const Identifier& getName() const noexcept { return name; }
    [[nodiscard]] std::string_view getDescription() const noexcept { return description; }
    [[nodiscard]] const std::function<NESTypeClass()>& getConstraintSupplier() const noexcept { return constraintSupplier; }
    [[nodiscard]] const std::function<std::expected<T, Exception>(const NESValue&)>& getFactory() const noexcept { return factory; }
    [[nodiscard]] const std::optional<std::function<T()>>& getDefaultSupplier() const noexcept { return defaultSupplier; }
};

/// Type-erased view of a `NESTypeParameter`, `NESOptionalTypeParameter`, `NESFunctorParameter`,
/// `NESOptionalFunctorParameter`, or `NESNonTypeParameter<T>` stored in `Schema`.
class QualifiedErasedNESTypeParameter
{
public:
    enum class Kind : uint8_t
    {
        TypeParameter,
        ValueParameter,
        FunctorParameter,
    };

private:
    QualifiedIdentifier name;
    NESParameterId originalParameterId;
    Kind kind;
    std::function<NESTypeClass()> constraintSupplier;
    std::function<std::expected<ExplicitAny, Exception>(const NESTypeParameters::Element&)> factory;
    std::optional<std::function<ExplicitAny()>> defaultSupplier;
    bool optionalWithoutDefault = false;
    std::string description;

public:
    QualifiedErasedNESTypeParameter(
        QualifiedIdentifier name,
        NESParameterId originalParameterId,
        Kind kind,
        std::function<NESTypeClass()> constraintSupplier,
        std::function<std::expected<ExplicitAny, Exception>(const NESTypeParameters::Element&)> factory,
        std::optional<std::function<ExplicitAny()>> defaultSupplier,
        bool optionalWithoutDefault,
        std::string description);

    [[nodiscard]] const QualifiedIdentifier& getFullyQualifiedName() const noexcept { return name; }
    [[nodiscard]] NESParameterId getOriginalParameterId() const noexcept { return originalParameterId; }
    [[nodiscard]] Kind getKind() const noexcept { return kind; }
    [[nodiscard]] NESTypeClass getConstraint() const { return constraintSupplier(); }
    [[nodiscard]] bool isOptional() const noexcept { return optionalWithoutDefault || defaultSupplier.has_value(); }
    [[nodiscard]] bool hasDefault() const noexcept { return defaultSupplier.has_value(); }
    [[nodiscard]] ExplicitAny getDefault() const;
    [[nodiscard]] std::expected<ExplicitAny, Exception> apply(const NESTypeParameters::Element& arg) const;
    [[nodiscard]] std::string_view getDescription() const noexcept { return description; }

    [[nodiscard]] bool operator==(const QualifiedErasedNESTypeParameter& other) const noexcept;
    friend std::ostream& operator<<(std::ostream& os, const QualifiedErasedNESTypeParameter& obj);
};

/// Single instantiated type/non-type/functor parameter entry in `Schema<InstantiatedNESTypeParameter, Ordered>`.
class InstantiatedNESTypeParameter
{
    QualifiedIdentifier name;
    NESParameterId originalParameterId;
    ExplicitAny value;

public:
    InstantiatedNESTypeParameter(QualifiedIdentifier name, NESParameterId originalParameterId, ExplicitAny value);

    [[nodiscard]] const QualifiedIdentifier& getFullyQualifiedName() const noexcept { return name; }
    [[nodiscard]] NESParameterId getOriginalParameterId() const noexcept { return originalParameterId; }
    [[nodiscard]] const ExplicitAny& getErasedValue() const noexcept { return value; }
    [[nodiscard]] bool operator==(const InstantiatedNESTypeParameter& other) const noexcept;
    friend std::ostream& operator<<(std::ostream& os, const InstantiatedNESTypeParameter& obj);
};

/// Validated, typed parameter map handed to functor and parameterized type-class `instantiate(...)` implementations,
/// providing type-safe lookup directly via the declared parameter objects (`params.get(ELEMENT_TYPE)`, `params.get(SIZE)`).
class InstantiatedNESTypeParameters
{
    std::unordered_map<NESParameterId, ExplicitAny, detail::NESParameterIdHash> values;

public:
    InstantiatedNESTypeParameters() = default;
    explicit InstantiatedNESTypeParameters(const Schema<InstantiatedNESTypeParameter, Ordered>& entries);

    [[nodiscard]] NESType get(const NESTypeParameter& param) const;
    [[nodiscard]] std::optional<NESType> get(const NESOptionalTypeParameter& param) const;
    [[nodiscard]] NESFunctor get(const NESFunctorParameter& param) const;
    [[nodiscard]] std::optional<NESFunctor> get(const NESOptionalFunctorParameter& param) const;

    template <typename T>
    [[nodiscard]] T get(const NESNonTypeParameter<T>& param) const
    {
        const auto* raw = findRaw(NESParameterId{param});
        PRECONDITION(raw != nullptr, "Could not find instantiated value for non-type parameter '{}'", param.getName());
        return raw->getAs<T>();
    }

    [[nodiscard]] size_t size() const noexcept { return values.size(); }
    [[nodiscard]] bool empty() const noexcept { return values.empty(); }

private:
    [[nodiscard]] const ExplicitAny* findRaw(NESParameterId id) const noexcept;
};

class ExpectedNESTypeArguments
{
    Schema<QualifiedErasedNESTypeParameter, Ordered> expectedParameters;

public:
    ExpectedNESTypeArguments() = default;
    explicit ExpectedNESTypeArguments(Schema<QualifiedErasedNESTypeParameter, Ordered> expectedParameters);

    [[nodiscard]] const Schema<QualifiedErasedNESTypeParameter, Ordered>& getParameters() const noexcept;
    [[nodiscard]] size_t size() const noexcept;
    [[nodiscard]] bool empty() const noexcept;

    /// Validates `passed` against parameter kinds, `NESTypeClass` constraints, and typed factories,
    /// returning an `InstantiatedNESTypeParameters` addressable directly via the declared parameter objects.
    [[nodiscard]] std::expected<InstantiatedNESTypeParameters, Exception> resolve(const NESTypeParameters& passed) const;

    [[nodiscard]] bool operator==(const ExpectedNESTypeArguments& other) const = default;
};

namespace detail
{
template <typename T>
constexpr bool IsNESNonTypeParameter = false;
template <typename R>
constexpr bool IsNESNonTypeParameter<NESNonTypeParameter<R>> = true;

template <typename C>
concept NESTypeSchemaComponent = std::same_as<std::remove_cvref_t<C>, NESTypeParameter>
    || std::same_as<std::remove_cvref_t<C>, NESOptionalTypeParameter>
    || std::same_as<std::remove_cvref_t<C>, NESFunctorParameter>
    || std::same_as<std::remove_cvref_t<C>, NESOptionalFunctorParameter>
    || IsNESNonTypeParameter<std::remove_cvref_t<C>>
    || std::same_as<std::remove_cvref_t<C>, ExpectedNESTypeArguments>;

inline std::string_view describeTypeArgumentKind(const NESTypeParameters::Element& arg) noexcept
{
    if (std::holds_alternative<NESType>(arg))
    {
        return "type";
    }
    if (std::holds_alternative<NESValue>(arg))
    {
        return "value";
    }
    return "functor";
}

inline std::vector<QualifiedErasedNESTypeParameter>
collectErasedTypeParameters(const std::optional<Identifier>& prefix, const NESTypeParameter& param)
{
    const QualifiedIdentifier fqName = prefix.has_value() ? QualifiedIdentifier{QualifiedIdentifier::create(*prefix, param.getName())}
                                                          : QualifiedIdentifier{QualifiedIdentifier::create(param.getName())};
    return {QualifiedErasedNESTypeParameter{
        fqName,
        NESParameterId{param},
        QualifiedErasedNESTypeParameter::Kind::TypeParameter,
        param.getConstraintSupplier(),
        [constraintSupplier = param.getConstraintSupplier(), fqName](const NESTypeParameters::Element& arg)
            -> std::expected<ExplicitAny, Exception>
        {
            const auto* typePtr = std::get_if<NESType>(&arg);
            if (typePtr == nullptr)
            {
                return std::unexpected{TypeInferenceException(
                    "Parameter '{}' expects a type argument, but received a {}", fqName, describeTypeArgumentKind(arg))};
            }
            if (!constraintSupplier().validate(*typePtr))
            {
                return std::unexpected{
                    TypeInferenceException("Type argument for parameter '{}' does not satisfy expected type class constraint", fqName)};
            }
            return ExplicitAny::create(*typePtr);
        },
        param.getDefaultSupplier().transform(
            [](std::function<NESType()> sup) -> std::function<ExplicitAny()>
            { return [sup = std::move(sup)] { return ExplicitAny::create(sup()); }; }),
        /*optionalWithoutDefault=*/false,
        std::string{param.getDescription()}}};
}

inline std::vector<QualifiedErasedNESTypeParameter>
collectErasedTypeParameters(const std::optional<Identifier>& prefix, const NESOptionalTypeParameter& param)
{
    const QualifiedIdentifier fqName = prefix.has_value() ? QualifiedIdentifier{QualifiedIdentifier::create(*prefix, param.getName())}
                                                          : QualifiedIdentifier{QualifiedIdentifier::create(param.getName())};
    return {QualifiedErasedNESTypeParameter{
        fqName,
        NESParameterId{param},
        QualifiedErasedNESTypeParameter::Kind::TypeParameter,
        param.getConstraintSupplier(),
        [constraintSupplier = param.getConstraintSupplier(), fqName](const NESTypeParameters::Element& arg)
            -> std::expected<ExplicitAny, Exception>
        {
            const auto* typePtr = std::get_if<NESType>(&arg);
            if (typePtr == nullptr)
            {
                return std::unexpected{TypeInferenceException(
                    "Parameter '{}' expects a type argument, but received a {}", fqName, describeTypeArgumentKind(arg))};
            }
            if (!constraintSupplier().validate(*typePtr))
            {
                return std::unexpected{
                    TypeInferenceException("Type argument for parameter '{}' does not satisfy expected type class constraint", fqName)};
            }
            return ExplicitAny::create(*typePtr);
        },
        std::nullopt,
        /*optionalWithoutDefault=*/true,
        std::string{param.getDescription()}}};
}

inline std::vector<QualifiedErasedNESTypeParameter>
collectErasedTypeParameters(const std::optional<Identifier>& prefix, const NESFunctorParameter& param)
{
    const QualifiedIdentifier fqName = prefix.has_value() ? QualifiedIdentifier{QualifiedIdentifier::create(*prefix, param.getName())}
                                                          : QualifiedIdentifier{QualifiedIdentifier::create(param.getName())};
    return {QualifiedErasedNESTypeParameter{
        fqName,
        NESParameterId{param},
        QualifiedErasedNESTypeParameter::Kind::FunctorParameter,
        &NESTypeClass::any,
        [validator = param.getValidator(), fqName](const NESTypeParameters::Element& arg) -> std::expected<ExplicitAny, Exception>
        {
            const auto* functorPtr = std::get_if<NESFunctor>(&arg);
            if (functorPtr == nullptr)
            {
                return std::unexpected{TypeInferenceException(
                    "Parameter '{}' expects a functor argument, but received a {}", fqName, describeTypeArgumentKind(arg))};
            }
            return validator(*functorPtr).transform([](NESFunctor&& f) { return ExplicitAny::create(std::move(f)); });
        },
        param.getDefaultSupplier().transform(
            [](std::function<NESFunctor()> sup) -> std::function<ExplicitAny()>
            { return [sup = std::move(sup)] { return ExplicitAny::create(sup()); }; }),
        /*optionalWithoutDefault=*/false,
        std::string{param.getDescription()}}};
}

inline std::vector<QualifiedErasedNESTypeParameter>
collectErasedTypeParameters(const std::optional<Identifier>& prefix, const NESOptionalFunctorParameter& param)
{
    const QualifiedIdentifier fqName = prefix.has_value() ? QualifiedIdentifier{QualifiedIdentifier::create(*prefix, param.getName())}
                                                          : QualifiedIdentifier{QualifiedIdentifier::create(param.getName())};
    return {QualifiedErasedNESTypeParameter{
        fqName,
        NESParameterId{param},
        QualifiedErasedNESTypeParameter::Kind::FunctorParameter,
        &NESTypeClass::any,
        [validator = param.getValidator(), fqName](const NESTypeParameters::Element& arg) -> std::expected<ExplicitAny, Exception>
        {
            const auto* functorPtr = std::get_if<NESFunctor>(&arg);
            if (functorPtr == nullptr)
            {
                return std::unexpected{TypeInferenceException(
                    "Parameter '{}' expects a functor argument, but received a {}", fqName, describeTypeArgumentKind(arg))};
            }
            return validator(*functorPtr).transform([](NESFunctor&& f) { return ExplicitAny::create(std::move(f)); });
        },
        std::nullopt,
        /*optionalWithoutDefault=*/true,
        std::string{param.getDescription()}}};
}

template <typename R>
std::vector<QualifiedErasedNESTypeParameter>
collectErasedTypeParameters(const std::optional<Identifier>& prefix, const NESNonTypeParameter<R>& param)
{
    const QualifiedIdentifier fqName = prefix.has_value() ? QualifiedIdentifier{QualifiedIdentifier::create(*prefix, param.getName())}
                                                          : QualifiedIdentifier{QualifiedIdentifier::create(param.getName())};
    return {QualifiedErasedNESTypeParameter{
        fqName,
        NESParameterId{param},
        QualifiedErasedNESTypeParameter::Kind::ValueParameter,
        param.getConstraintSupplier(),
        [constraintSupplier = param.getConstraintSupplier(), factory = param.getFactory(), fqName](
            const NESTypeParameters::Element& arg) -> std::expected<ExplicitAny, Exception>
        {
            const auto* valPtr = std::get_if<NESValue>(&arg);
            if (valPtr == nullptr)
            {
                return std::unexpected{TypeInferenceException(
                    "Parameter '{}' expects a constant value argument, but received a {}", fqName, describeTypeArgumentKind(arg))};
            }
            if (!constraintSupplier().validate(valPtr->getType()))
            {
                return std::unexpected{TypeInferenceException("Value argument for parameter '{}' has invalid type", fqName)};
            }
            return factory(*valPtr).transform([](R&& v) { return ExplicitAny::create(std::move(v)); });
        },
        param.getDefaultSupplier().transform(
            [](std::function<R()> sup) -> std::function<ExplicitAny()>
            { return [sup = std::move(sup)] { return ExplicitAny::create(sup()); }; }),
        /*optionalWithoutDefault=*/false,
        std::string{param.getDescription()}}};
}

inline std::vector<QualifiedErasedNESTypeParameter>
collectErasedTypeParameters(const std::optional<Identifier>& prefix, const ExpectedNESTypeArguments& nested)
{
    std::vector<QualifiedErasedNESTypeParameter> out;
    for (const auto& item : nested.getParameters())
    {
        if (!prefix.has_value())
        {
            out.push_back(item);
        }
        else
        {
            auto prefixedIds = std::vector<Identifier>{*prefix};
            for (const auto& part : item.getFullyQualifiedName())
            {
                prefixedIds.push_back(part);
            }
            out.emplace_back(
                QualifiedIdentifier::create(std::move(prefixedIds)),
                item.getOriginalParameterId(),
                item.getKind(),
                [c = item.getConstraint()] { return c; },
                [item](const NESTypeParameters::Element& arg) { return item.apply(arg); },
                item.hasDefault() ? std::optional<std::function<ExplicitAny()>>{[item] { return item.getDefault(); }} : std::nullopt,
                item.isOptional() && !item.hasDefault(),
                std::string{item.getDescription()});
        }
    }
    return out;
}
}

template <detail::NESTypeSchemaComponent... Components>
ExpectedNESTypeArguments createExpectedNESTypeArguments(const Components&... components)
{
    std::vector<QualifiedErasedNESTypeParameter> params;
    (
        [&]
        {
            for (auto&& p : detail::collectErasedTypeParameters(std::nullopt, components))
            {
                params.push_back(std::move(p));
            }
        }(),
        ...);
    return ExpectedNESTypeArguments{Schema<QualifiedErasedNESTypeParameter, Ordered>{std::move(params)}};
}

template <detail::NESTypeSchemaComponent... Components>
ExpectedNESTypeArguments createExpectedNESTypeArguments(const Identifier& prefix, const Components&... components)
{
    std::vector<QualifiedErasedNESTypeParameter> params;
    (
        [&]
        {
            for (auto&& p : detail::collectErasedTypeParameters(prefix, components))
            {
                params.push_back(std::move(p));
            }
        }(),
        ...);
    return ExpectedNESTypeArguments{Schema<QualifiedErasedNESTypeParameter, Ordered>{std::move(params)}};
}

/// Concept fulfilled by concrete C++ parameterized type-class constructors wrapped in `NESParametrizedTypeClass`.
/// Supports either:
/// - `getExpectedArguments() -> ExpectedNESTypeArguments` + `instantiate(const InstantiatedNESTypeParameters&)`, or
/// - `instantiate(const NESTypeClassParameters&)`.
namespace detail
{
template <typename T>
concept RawParametrizedTypeClassImpl = requires(const std::remove_cvref_t<T>& self, const NESTypeClassParameters& params) {
    { self.instantiate(params) } -> std::same_as<std::expected<NESTypeClass, Exception>>;
};

template <typename T>
concept SchemaParametrizedTypeClassImpl = requires(const std::remove_cvref_t<T>& self, const InstantiatedNESTypeParameters& params) {
    { self.getExpectedArguments() } -> std::convertible_to<ExpectedNESTypeArguments>;
    { self.instantiate(params) } -> std::same_as<std::expected<NESTypeClass, Exception>>;
};
}

template <typename T>
concept NESParametrizedTypeClassConcept = (!std::same_as<std::remove_cvref_t<T>, NESParametrizedTypeClass>)
    && std::equality_comparable<std::remove_cvref_t<T>>
    && (detail::RawParametrizedTypeClassImpl<T> || detail::SchemaParametrizedTypeClassImpl<T>);

namespace detail
{
struct ErasedNESParametrizedTypeClass
{
    virtual ~ErasedNESParametrizedTypeClass() = default;
    [[nodiscard]] virtual std::expected<NESTypeClass, Exception> instantiate(const NESTypeClassParameters& params) const = 0;
    [[nodiscard]] virtual const std::optional<ExpectedNESTypeArguments>& getExpectedArguments() const noexcept = 0;
    [[nodiscard]] virtual bool equals(const ErasedNESParametrizedTypeClass& other) const = 0;
};

template <NESParametrizedTypeClassConcept T>
struct NESParametrizedTypeClassModel final : ErasedNESParametrizedTypeClass
{
    T impl;
    std::optional<ExpectedNESTypeArguments> expectedArguments;

    static std::optional<ExpectedNESTypeArguments> extractExpectedArguments(const T& source)
    {
        if constexpr (SchemaParametrizedTypeClassImpl<T>)
        {
            return source.getExpectedArguments();
        }
        else
        {
            (void)source;
            return std::nullopt;
        }
    }

    explicit NESParametrizedTypeClassModel(T impl)
        : impl(std::move(impl))
        , expectedArguments(extractExpectedArguments(this->impl))
    {
    }

    [[nodiscard]] std::expected<NESTypeClass, Exception> instantiate(const NESTypeClassParameters& params) const override
    {
        if constexpr (SchemaParametrizedTypeClassImpl<T>)
        {
            auto resolvedExp = expectedArguments->resolve(params.getParameters());
            if (!resolvedExp.has_value())
            {
                return std::unexpected{std::move(resolvedExp).error()};
            }
            return impl.instantiate(*resolvedExp);
        }
        else
        {
            return impl.instantiate(params);
        }
    }

    [[nodiscard]] const std::optional<ExpectedNESTypeArguments>& getExpectedArguments() const noexcept override
    {
        return expectedArguments;
    }

    [[nodiscard]] bool equals(const ErasedNESParametrizedTypeClass& other) const override
    {
        if (const auto* otherModel = dynamic_cast<const NESParametrizedTypeClassModel<T>*>(&other))
        {
            return impl == otherModel->impl;
        }
        return false;
    }
};
}

/// A parameterized constructor for a `NESTypeClass` (corresponds to a templated `concept` in C++).
/// When a `NESParametrizedTypeClass` takes 0 parameters, `unparametrized()` yields its unique `NESTypeClass`.
class NESParametrizedTypeClass
{
    std::shared_ptr<const detail::ErasedNESParametrizedTypeClass> self;

public:
    template <NESParametrizedTypeClassConcept T>
    explicit NESParametrizedTypeClass(T impl)
        : self(std::make_shared<detail::NESParametrizedTypeClassModel<std::remove_cvref_t<T>>>(std::move(impl)))
    {
    }

    static NESParametrizedTypeClass constant(NESTypeClass constantTypeClass);

    template <typename T>
    [[nodiscard]] const std::remove_cvref_t<T>* tryGetAs() const noexcept
    {
        using CleanT = std::remove_cvref_t<T>;
        if constexpr (NESParametrizedTypeClassConcept<CleanT>)
        {
            if (const auto* model = dynamic_cast<const detail::NESParametrizedTypeClassModel<CleanT>*>(self.get()))
            {
                return &model->impl;
            }
        }
        return nullptr;
    }

    [[nodiscard]] std::expected<NESTypeClass, Exception> instantiate(const NESTypeClassParameters& params = {}) const;
    [[nodiscard]] const std::optional<ExpectedNESTypeArguments>& getExpectedArguments() const noexcept;
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
    [[nodiscard]] const std::optional<ExpectedNESTypeArguments>& getExpectedArguments() const noexcept;
    [[nodiscard]] NESTypeClass unparametrized() const;
    [[nodiscard]] bool operator==(const NamedNESParametrizedTypeClass& other) const;
    friend std::ostream& operator<<(std::ostream& os, const NamedNESParametrizedTypeClass& obj);
};

/// Declares a single formal value parameter on a constructible `NESType` (`NESType::construct(...)`),
/// mirroring `ConfigField<T>` in `nes-configurations`.
template <typename T = NESValue>
class NESValueParameter
{
    Identifier name;
    std::string description;
    std::function<NESTypeClass()> constraintSupplier;
    std::function<std::expected<T, Exception>(const NESValue&)> factory;
    std::optional<std::function<T()>> defaultSupplier;

public:
    NESValueParameter(Identifier name, std::string description, NESTypeClass constraint = NESTypeClass::any())
        : name(std::move(name))
        , description(std::move(description))
        , constraintSupplier([c = std::move(constraint)] { return c; })
        , factory([paramName = this->name](const NESValue& val) { return detail::defaultExtractFromNESValue<T>(val, paramName); })
        , defaultSupplier(std::nullopt)
    {
    }

    NESValueParameter(Identifier name, std::string description, std::function<NESTypeClass()> constraintSupplier)
        : name(std::move(name))
        , description(std::move(description))
        , constraintSupplier(std::move(constraintSupplier))
        , factory([paramName = this->name](const NESValue& val) { return detail::defaultExtractFromNESValue<T>(val, paramName); })
        , defaultSupplier(std::nullopt)
    {
    }

    NESValueParameter(
        Identifier name,
        std::string description,
        std::function<std::expected<T, Exception>(const NESValue&)> factory)
        : name(std::move(name))
        , description(std::move(description))
        , constraintSupplier([] { return NESTypeClass::any(); })
        , factory(std::move(factory))
        , defaultSupplier(std::nullopt)
    {
    }

    NESValueParameter(
        Identifier name,
        std::string description,
        NESTypeClass constraint,
        std::function<std::expected<T, Exception>(const NESValue&)> factory)
        : name(std::move(name))
        , description(std::move(description))
        , constraintSupplier([c = std::move(constraint)] { return c; })
        , factory(std::move(factory))
        , defaultSupplier(std::nullopt)
    {
    }

    NESValueParameter(
        Identifier name,
        std::string description,
        NESTypeClass constraint,
        std::function<std::expected<T, Exception>(const NESValue&)> factory,
        T defaultValue)
        : name(std::move(name))
        , description(std::move(description))
        , constraintSupplier([c = std::move(constraint)] { return c; })
        , factory(std::move(factory))
        , defaultSupplier([defaultValue = std::move(defaultValue)] { return defaultValue; })
    {
    }

    NESValueParameter(const NESValueParameter&) = delete;
    NESValueParameter(NESValueParameter&&) noexcept = delete;
    NESValueParameter& operator=(const NESValueParameter&) = delete;
    NESValueParameter& operator=(NESValueParameter&&) noexcept = delete;
    ~NESValueParameter() = default;

    [[nodiscard]] const Identifier& getName() const noexcept { return name; }
    [[nodiscard]] std::string_view getDescription() const noexcept { return description; }
    [[nodiscard]] const std::function<NESTypeClass()>& getConstraintSupplier() const noexcept { return constraintSupplier; }
    [[nodiscard]] const std::function<std::expected<T, Exception>(const NESValue&)>& getFactory() const noexcept { return factory; }
    [[nodiscard]] const std::optional<std::function<T()>>& getDefaultSupplier() const noexcept { return defaultSupplier; }
};

class QualifiedErasedNESValueParameter
{
    QualifiedIdentifier name;
    NESParameterId originalParameterId;
    std::function<NESTypeClass()> constraintSupplier;
    std::function<std::expected<ExplicitAny, Exception>(const NESValue&)> factory;
    std::optional<std::function<ExplicitAny()>> defaultSupplier;
    std::string description;

public:
    QualifiedErasedNESValueParameter(
        QualifiedIdentifier name,
        NESParameterId originalParameterId,
        std::function<NESTypeClass()> constraintSupplier,
        std::function<std::expected<ExplicitAny, Exception>(const NESValue&)> factory,
        std::optional<std::function<ExplicitAny()>> defaultSupplier,
        std::string description);

    [[nodiscard]] const QualifiedIdentifier& getFullyQualifiedName() const noexcept { return name; }
    [[nodiscard]] NESParameterId getOriginalParameterId() const noexcept { return originalParameterId; }
    [[nodiscard]] NESTypeClass getConstraint() const { return constraintSupplier(); }
    [[nodiscard]] bool hasDefault() const noexcept { return defaultSupplier.has_value(); }
    [[nodiscard]] ExplicitAny getDefault() const;
    [[nodiscard]] std::expected<ExplicitAny, Exception> apply(const NESValue& val) const;
    [[nodiscard]] std::string_view getDescription() const noexcept { return description; }

    [[nodiscard]] bool operator==(const QualifiedErasedNESValueParameter& other) const noexcept;
    friend std::ostream& operator<<(std::ostream& os, const QualifiedErasedNESValueParameter& obj);
};

class InstantiatedNESValueParameter
{
    QualifiedIdentifier name;
    NESParameterId originalParameterId;
    ExplicitAny value;

public:
    InstantiatedNESValueParameter(QualifiedIdentifier name, NESParameterId originalParameterId, ExplicitAny value);

    [[nodiscard]] const QualifiedIdentifier& getFullyQualifiedName() const noexcept { return name; }
    [[nodiscard]] NESParameterId getOriginalParameterId() const noexcept { return originalParameterId; }
    [[nodiscard]] const ExplicitAny& getErasedValue() const noexcept { return value; }
    [[nodiscard]] bool operator==(const InstantiatedNESValueParameter& other) const noexcept;
    friend std::ostream& operator<<(std::ostream& os, const InstantiatedNESValueParameter& obj);
};

class InstantiatedNESValueParameters
{
    std::unordered_map<NESParameterId, ExplicitAny, detail::NESParameterIdHash> values;

public:
    InstantiatedNESValueParameters() = default;
    explicit InstantiatedNESValueParameters(const Schema<InstantiatedNESValueParameter, Ordered>& entries);

    template <typename T>
    [[nodiscard]] T get(const NESValueParameter<T>& param) const
    {
        const auto* raw = findRaw(NESParameterId{param});
        PRECONDITION(raw != nullptr, "Could not find instantiated value for parameter '{}'", param.getName());
        return raw->getAs<T>();
    }

    [[nodiscard]] size_t size() const noexcept { return values.size(); }
    [[nodiscard]] bool empty() const noexcept { return values.empty(); }

private:
    [[nodiscard]] const ExplicitAny* findRaw(NESParameterId id) const noexcept;
};

class ExpectedNESValueArguments
{
    Schema<QualifiedErasedNESValueParameter, Ordered> expectedParameters;

public:
    ExpectedNESValueArguments() = default;
    explicit ExpectedNESValueArguments(Schema<QualifiedErasedNESValueParameter, Ordered> expectedParameters);

    [[nodiscard]] const Schema<QualifiedErasedNESValueParameter, Ordered>& getParameters() const noexcept;
    [[nodiscard]] size_t size() const noexcept;
    [[nodiscard]] bool empty() const noexcept;

    [[nodiscard]] std::expected<InstantiatedNESValueParameters, Exception> resolve(const NESValueParameters& passed) const;
    [[nodiscard]] bool operator==(const ExpectedNESValueArguments& other) const = default;
};

namespace detail
{
template <typename T>
constexpr bool IsNESValueParameter = false;
template <typename R>
constexpr bool IsNESValueParameter<NESValueParameter<R>> = true;

template <typename C>
concept NESValueSchemaComponent
    = IsNESValueParameter<std::remove_cvref_t<C>> || std::same_as<std::remove_cvref_t<C>, ExpectedNESValueArguments>;

template <typename R>
std::vector<QualifiedErasedNESValueParameter>
collectErasedValueParameters(const std::optional<Identifier>& prefix, const NESValueParameter<R>& param)
{
    const QualifiedIdentifier fqName = prefix.has_value() ? QualifiedIdentifier{QualifiedIdentifier::create(*prefix, param.getName())}
                                                          : QualifiedIdentifier{QualifiedIdentifier::create(param.getName())};
    return {QualifiedErasedNESValueParameter{
        fqName,
        NESParameterId{param},
        param.getConstraintSupplier(),
        [constraintSupplier = param.getConstraintSupplier(), factory = param.getFactory(), fqName](const NESValue& val)
            -> std::expected<ExplicitAny, Exception>
        {
            if (!constraintSupplier().validate(val.getType()))
            {
                return std::unexpected{
                    InvalidQuerySyntax("Argument for parameter '{}' does not satisfy expected type class constraint", fqName)};
            }
            return factory(val).transform([](R&& v) { return ExplicitAny::create(std::move(v)); });
        },
        param.getDefaultSupplier().transform(
            [](std::function<R()> sup) -> std::function<ExplicitAny()>
            { return [sup = std::move(sup)] { return ExplicitAny::create(sup()); }; }),
        std::string{param.getDescription()}}};
}

inline std::vector<QualifiedErasedNESValueParameter>
collectErasedValueParameters(const std::optional<Identifier>& prefix, const ExpectedNESValueArguments& nested)
{
    std::vector<QualifiedErasedNESValueParameter> out;
    for (const auto& item : nested.getParameters())
    {
        if (!prefix.has_value())
        {
            out.push_back(item);
        }
        else
        {
            auto prefixedIds = std::vector<Identifier>{*prefix};
            for (const auto& part : item.getFullyQualifiedName())
            {
                prefixedIds.push_back(part);
            }
            out.emplace_back(
                QualifiedIdentifier::create(std::move(prefixedIds)),
                item.getOriginalParameterId(),
                [c = item.getConstraint()] { return c; },
                [item](const NESValue& val) { return item.apply(val); },
                item.hasDefault() ? std::optional<std::function<ExplicitAny()>>{[item] { return item.getDefault(); }} : std::nullopt,
                std::string{item.getDescription()});
        }
    }
    return out;
}
}

template <detail::NESValueSchemaComponent... Components>
ExpectedNESValueArguments createExpectedNESValueArguments(const Components&... components)
{
    std::vector<QualifiedErasedNESValueParameter> params;
    (
        [&]
        {
            for (auto&& p : detail::collectErasedValueParameters(std::nullopt, components))
            {
                params.push_back(std::move(p));
            }
        }(),
        ...);
    return ExpectedNESValueArguments{Schema<QualifiedErasedNESValueParameter, Ordered>{std::move(params)}};
}

namespace detail
{
template <typename T>
struct NESTypeModel final : ErasedNESType
{
    using ValueRepr = typename DeducedNESValueRepr<T>::Type;
    T impl;

    explicit NESTypeModel(T impl) : impl(std::move(impl)) { }

    [[nodiscard]] std::expected<ExplicitAny, Exception> constructRaw(const NESValueParameters& params) const override
    {
        if constexpr (SchemaConstructibleNESTypeImpl<T>)
        {
            auto resolvedExp = impl.getExpectedValueArguments().resolve(params);
            if (!resolvedExp.has_value())
            {
                return std::unexpected{std::move(resolvedExp).error()};
            }
            if constexpr (std::same_as<ValueRepr, ExplicitAny>)
            {
                return impl.construct(*resolvedExp);
            }
            else
            {
                return impl.construct(*resolvedExp)
                    .transform([](auto&& repr) { return ExplicitAny::create(std::forward<decltype(repr)>(repr)); });
            }
        }
        else if constexpr (RawConstructibleNESTypeImpl<T>)
        {
            if constexpr (std::same_as<ValueRepr, ExplicitAny>)
            {
                return impl.construct(params);
            }
            else
            {
                return impl.construct(params).transform(
                    [](auto&& repr) { return ExplicitAny::create(std::forward<decltype(repr)>(repr)); });
            }
        }
        else
        {
            (void)params;
            return std::unexpected{
                TypeInferenceException("Values of this NESType cannot be constructed directly from arguments")};
        }
    }

    [[nodiscard]] bool equals(const ErasedNESType& other) const override
    {
        if (const auto* otherModel = dynamic_cast<const NESTypeModel<T>*>(&other))
        {
            return impl == otherModel->impl;
        }
        return false;
    }

    [[nodiscard]] bool equalsValue(const ExplicitAny& lhs, const ExplicitAny& rhs) const override
    {
        if constexpr (HasCustomEqualsValue<T>)
        {
            return impl.equalsValue(lhs, rhs);
        }
        else
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
        }
    }
};
}

template <NESTypeConcept T>
NESType::NESType(T typeImpl)
    : self(std::make_shared<detail::NESTypeModel<std::remove_cvref_t<T>>>(std::move(typeImpl)))
    , typeTraits(nullptr)
{
}

template <typename T>
const std::remove_cvref_t<T>* NESType::tryGetAs() const noexcept
{
    using CleanT = std::remove_cvref_t<T>;
    if (const auto* model = dynamic_cast<const detail::NESTypeModel<CleanT>*>(self.get()))
    {
        return &model->impl;
    }
    return nullptr;
}

namespace detail
{
template <typename T>
concept HasExpectedNESTypeArguments = requires(const T& self) {
    { self.getExpectedArguments() } -> std::convertible_to<std::optional<ExpectedNESTypeArguments>>;
};

template <typename T>
concept HasRegistryEntry = requires(const T& self) {
    self.getRegistryEntry();
};

template <NESFunctorConcept T>
struct NESFunctorModel final : ErasedNESFunctor
{
    T impl;
    std::optional<ExpectedNESTypeArguments> expectedArguments;
    std::optional<ExplicitAny> registryEntry;

    static std::optional<ExpectedNESTypeArguments> extractExpectedArguments(const T& source)
    {
        if constexpr (HasExpectedNESTypeArguments<T>)
        {
            return source.getExpectedArguments();
        }
        else
        {
            (void)source;
            return std::nullopt;
        }
    }

    static std::optional<ExplicitAny> extractRegistryEntry(const T& source)
    {
        if constexpr (HasRegistryEntry<T>)
        {
            using EntryT = std::remove_cvref_t<decltype(source.getRegistryEntry())>;
            return ExplicitAny::create(EntryT{source.getRegistryEntry()});
        }
        else
        {
            (void)source;
            return std::nullopt;
        }
    }

    explicit NESFunctorModel(T impl)
        : impl(std::move(impl))
        , expectedArguments(extractExpectedArguments(this->impl))
        , registryEntry(extractRegistryEntry(this->impl))
    {
    }

    [[nodiscard]] std::expected<NESType, Exception> instantiateRaw(const NESTypeParameters& params) const override
    {
        if constexpr (SchemaFunctorImpl<T>)
        {
            auto resolvedExp = expectedArguments->resolve(params);
            if (!resolvedExp.has_value())
            {
                return std::unexpected{std::move(resolvedExp).error()};
            }
            return impl.instantiate(*resolvedExp);
        }
        else
        {
            if (expectedArguments.has_value())
            {
                auto resolvedExp = expectedArguments->resolve(params);
                if (!resolvedExp.has_value())
                {
                    return std::unexpected{std::move(resolvedExp).error()};
                }
            }
            return impl.instantiate(params);
        }
    }

    [[nodiscard]] const std::optional<ExpectedNESTypeArguments>& getExpectedArguments() const noexcept override
    {
        return expectedArguments;
    }

    [[nodiscard]] const ExplicitAny* tryGetRegistryEntry() const noexcept override
    {
        return registryEntry.has_value() ? &*registryEntry : nullptr;
    }

    [[nodiscard]] bool equals(const ErasedNESFunctor& other) const override
    {
        if (const auto* otherModel = dynamic_cast<const NESFunctorModel<T>*>(&other))
        {
            return impl == otherModel->impl;
        }
        return false;
    }
};
}

template <NESFunctorConcept T>
NESFunctor::NESFunctor(T functorImpl)
    : self(std::make_shared<detail::NESFunctorModel<std::remove_cvref_t<T>>>(std::move(functorImpl)))
{
}

template <typename T>
const std::remove_cvref_t<T>* NESFunctor::tryGetAs() const noexcept
{
    using CleanT = std::remove_cvref_t<T>;
    if constexpr (NESFunctorConcept<CleanT>)
    {
        if (const auto* model = dynamic_cast<const detail::NESFunctorModel<CleanT>*>(self.get()))
        {
            return &model->impl;
        }
    }
    return nullptr;
}

template <typename EntryT>
const std::remove_cvref_t<EntryT>* NESFunctor::tryGetRegistryEntryAs() const noexcept
{
    using CleanEntryT = std::remove_cvref_t<EntryT>;
    if constexpr (std::same_as<CleanEntryT, NESFunctor>)
    {
        return this;
    }
    else
    {
        if (const auto* entryAny = self->tryGetRegistryEntry())
        {
            return entryAny->tryGetAs<CleanEntryT>();
        }
        return tryGetAs<CleanEntryT>();
    }
}

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
struct std::hash<NES::NESParameterId>
{
    size_t operator()(const NES::NESParameterId& id) const noexcept { return std::hash<uintptr_t>{}(id.rawAddress()); }
};

template <>
struct std::hash<NES::QualifiedErasedNESTypeParameter>
{
    size_t operator()(const NES::QualifiedErasedNESTypeParameter& val) const noexcept
    {
        return std::hash<NES::QualifiedIdentifier>{}(val.getFullyQualifiedName());
    }
};

template <>
struct std::hash<NES::InstantiatedNESTypeParameter>
{
    size_t operator()(const NES::InstantiatedNESTypeParameter& val) const noexcept
    {
        return std::hash<NES::QualifiedIdentifier>{}(val.getFullyQualifiedName());
    }
};

template <>
struct std::hash<NES::QualifiedErasedNESValueParameter>
{
    size_t operator()(const NES::QualifiedErasedNESValueParameter& val) const noexcept
    {
        return std::hash<NES::QualifiedIdentifier>{}(val.getFullyQualifiedName());
    }
};

template <>
struct std::hash<NES::InstantiatedNESValueParameter>
{
    size_t operator()(const NES::InstantiatedNESValueParameter& val) const noexcept
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
FMT_OSTREAM(NES::NESParameterId);
FMT_OSTREAM(NES::QualifiedErasedNESTypeParameter);
FMT_OSTREAM(NES::InstantiatedNESTypeParameter);
FMT_OSTREAM(NES::QualifiedErasedNESValueParameter);
FMT_OSTREAM(NES::InstantiatedNESValueParameter);
FMT_OSTREAM(NES::NamedNESFunctor);
