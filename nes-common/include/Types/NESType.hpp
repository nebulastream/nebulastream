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
#include "Identifiers/QualifiedIdentifier.hpp"
#include <Schema/Schema.hpp>
#include "Util/Any.hpp"

#include "NESType.hpp"

namespace NES
{

class NESValue;
class NamedNESValue;
class NamedNESType;

/// Example: LogicalOperator as the NESTypeClassStructure, a LogicalOperator with a set output schema is a NESTypeClass,
/// a ProjectionLogicalOperator is a NESTypeConstructor, a ProjectionLogicalOperator with an output schema is a NESType,
/// and instance of it is a NESValue


/// A fully instantiated type (compares to a fully instantiated template in c++)
/// For example, an array<varchar, 5> is a type
class NESType
{
    ExplicitAny typeDescriptor;
    std::function<NESValue(const std::vector<NamedNESValue>&)> constructor;
    std::function<bool(const ExplicitAny&, const ExplicitAny&)> typeEquals;
    std::function<bool(const ExplicitAny&, const ExplicitAny&)> valueEquals;

public:
    NESType(
        ExplicitAny typeDescriptor,
        std::function<NESValue(const std::vector<NamedNESValue>&)> constructor,
        std::function<bool(const ExplicitAny&, const ExplicitAny&)> typeEquals,
        std::function<bool(const ExplicitAny&, const ExplicitAny&)> valueEquals);

    explicit NESType(NamedNESType newType);

    [[nodiscard]] NESValue construct(const std::vector<NESValue>&) const;
    bool operator==(const NESType&) const;

    [[nodiscard]] const ExplicitAny& getDescriptor() const;
};

/// An actual instance of some type. For example, an instance of an logical operator, logical function, etc.
class NESValue
{
    NESType type;
    ExplicitAny internalRepresentation;

public:
    explicit NESValue(NESType type, ExplicitAny internalRepresentation);
    [[nodiscard]] const ExplicitAny& getInternalRepresentation() const;
    [[nodiscard]] const NESType& getType() const;
    bool operator==(const NESValue&) const;
};

class NamedNESTypeParameter
{
    std::variant<NESType, NESValue> value;
    QualifiedIdentifier name;

public:
    explicit NamedNESTypeParameter(NESType type, QualifiedIdentifier name);

    [[nodiscard]] const QualifiedIdentifier& getFullyQualifiedName() const;
};


/// Blackbox argument to a type constructor
/// Type is a bit of an placeholder, might get removed or expanded in the future
class PassedNESTypeParameters
{
    Schema<NamedNESTypeParameter, Ordered> parameters;
    //std::function<bool(const ExplicitAny&, const ExplicitAny&)> typeParametersEquals;
public:
    explicit NESTypeParameters(Schema<NamedNESTypeParameter, Ordered> parameters);

    /// throwing if not found as expected for now
    [[nodiscard]] const NamedNESTypeParameter& getParameters() const;
    /// TODO To SFINAE or not to SFINAE?
};

struct RegistryProvision;

/// A type constructor.
/// Example: A scalar array type, that takes another scalar type and a number as parameters.
/// Similar to c++ type templates, but we don't have separate arguments but just one blackbox argument that the type constructor needs to be able to understand.
class NESFunctor
{
    ExplicitAny structure;
    std::function<bool(const ExplicitAny&, const ExplicitAny&)> typeStructureEquals;
    std::function<NESType(const ExplicitAny&, const NESTypeParameters&)> typeConstructor;
    template <typename T>
    friend RegistryProvision provideRegistry();

public:
    template <typename Descriptor>
    explicit NESFunctor(
        Descriptor descriptor,
        std::type_identity_t<std::function<bool(const Descriptor&, const Descriptor&)>> typeStructureEquals,
        std::type_identity_t<std::function<NESType(const Descriptor&, const NESTypeParameters&)>> typeConstructor)
        : structure(ExplicitAny{std::any{std::move(descriptor)}})
        , typeStructureEquals(
              [typeStructureEquals = std::move(typeStructureEquals)](const ExplicitAny& self, const ExplicitAny& other)
              {
                  const auto* selfCasted = std::any_cast<const Descriptor*>(&self.getUnderlying());
                  const auto* otherCasted = std::any_cast<const Descriptor*>(&other.getUnderlying());
                  if (selfCasted != nullptr && otherCasted != nullptr)
                  {
                      return typeStructureEquals(*selfCasted, *otherCasted);
                  }
                  return false;
              })
        , typeConstructor([typeConstructor = std::move(typeConstructor)](const ExplicitAny& self, const NESTypeParameters& other)
                          { return typeConstructor(self.getAs<const Descriptor&>(), other); })
    {
    }
};

/// A type with a name, which is considered NOT equals to a type with the same structure but different name
class NamedNESFunctor
{
    QualifiedIdentifier name;
    NESFunctor underlyingType;

public:
    explicit NamedNESFunctor(QualifiedIdentifier name, NESFunctor underlyingType)
        : name(std::move(name)), underlyingType(std::move(underlyingType))
    {
    }

    [[nodiscard]] const QualifiedIdentifier& getFullyQualifiedName() const;
    [[nodiscard]] const std::shared_ptr<NESFunctor>& getUnderlyingFunctor() const;

    bool operator==(const NamedNESType&) const;
};

class NESTypeClassParameters
{
    ExplicitAny parameters;
    std::function<bool(const ExplicitAny&, const ExplicitAny&)> typeClassParamsEquals;
};

class NESTypeClass;

/// A parametrized constructor for a type class, which is a predicate over types
/// Corresponds to a templated concept in c++
/// In NES, the registries all imply one NESTypeClassStructure.
/// If a NESTypeClassStructure doesn't take any arguments, it corresponds 1:1 with a NESTypeClass
class NESParametrizedTypeClass
{
    //std::shared_ptr<const ExplicitAny> structure;
    //std::function<bool(const ExplicitAny&, const ExplicitAny&)> typeClassStructureEquals;
    std::function<NESTypeClass(const NESTypeClassParameters&)> typeClassConstructor;

public:
    explicit NESParametrizedTypeClass(std::function<NESTypeClass(const NESTypeClassParameters&)> typeClassConstructor);
};

class NamedNESParametrizedTypeClass
{
    QualifiedIdentifier name;
    NESParametrizedTypeClass underlying;

public:
    NamedNESParametrizedTypeClass(QualifiedIdentifier name, NESParametrizedTypeClass newType);

    [[nodiscard]] const QualifiedIdentifier& getFullyQualifiedName() const;

    bool operator==(const NamedNESParametrizedTypeClass&) const;
};

/// A predicate over types
class NESTypeClass
{
    ExplicitAny typeClassDescriptor;
    std::function<bool(const ExplicitAny&, const NESType&)> predicate;
    std::function<bool(const ExplicitAny&, const ExplicitAny&)> typeClassEquals;

    NESTypeClass(
        ExplicitAny typeClassDescriptor,
        std::function<bool(const ExplicitAny&, const NESType&)> predicate,
        std::function<bool(const ExplicitAny&, const ExplicitAny&)> typeClassEquals)
        : typeClassDescriptor(std::move(typeClassDescriptor)), predicate(std::move(predicate)), typeClassEquals(std::move(typeClassEquals))
    {
    }

public:
    template <typename Descriptor>
    NESTypeClass(
        Descriptor typeClassDescriptor,
        std::type_identity_t<std::function<bool(const Descriptor&, const NESType&)>> predicate,
        std::type_identity_t<std::function<bool(const Descriptor&, const Descriptor&)>> typeClassEquals)
        : typeClassDescriptor(ExplicitAny{std::any{std::move(typeClassDescriptor)}})
        , predicate(
              [predicate = std::move(predicate)](const ExplicitAny& descriptor, const NESType& type)
              {
                  if (const Descriptor* casted = any_cast<const Descriptor*>(&descriptor.getUnderlying()))
                  {
                      return predicate(*casted, type);
                  }
                  return false;
              })
        , typeClassEquals(
              [typeClassEquals = std::move(typeClassEquals)](const ExplicitAny& self, const ExplicitAny& other)
              {
                  const Descriptor* castedSelf = any_cast<const Descriptor*>(&self.getUnderlying());
                  const Descriptor* castedOther = any_cast<const Descriptor*>(&other.getUnderlying());
                  if (castedSelf != nullptr && castedOther != nullptr)
                  {
                      return typeClassEquals(*castedOther, *castedSelf);
                  }
                  return false;
              })
    {
    }

    [[nodiscard]] bool validate(const NESType& type) const;
    bool operator==(const NESTypeClass&) const;
};

class NamedNESTypeClass
{
    QualifiedIdentifier name;
    NESTypeClass typeClass;
public:
    [[nodiscard]] const QualifiedIdentifier& getFullyQualifiedName() const;
    bool operator==(const NamedNESTypeClass&) const;
};

class ExpectedNESTypeArguments
{
    Schema<NamedNESTypeClass, Ordered> expectedTypeClasses;

};
}
