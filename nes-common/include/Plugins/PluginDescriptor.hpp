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
#include <cstdint>
#include <expected>
#include <functional>
#include <memory>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <Identifiers/Identifier.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Types/NESType.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>

namespace NES
{

enum class InsertResult : uint8_t
{
    Success,
    DuplicateKey,
    EntryTypeMismatch,
};

/// Default `NESType`, `NESTypeClass`, and `NESFunctor` implementations for nullary plugin registries
/// whose entries wrap `Registry::EntryType` directly without custom SQL-level type parameters.
template <typename Registry>
struct DefaultRegistryNESType
{
    std::shared_ptr<const int> identityToken;
    typename Registry::EntryType entry;

    [[nodiscard]] std::expected<typename Registry::EntryType, Exception> construct(const NESValueParameters& params) const
    {
        if (!params.empty())
        {
            return std::unexpected{
                InvalidQuerySyntax("Nullary registry type expects 0 value arguments, but got {}", params.size())};
        }
        return entry;
    }

    bool operator==(const DefaultRegistryNESType& other) const
    {
        if (identityToken == other.identityToken)
        {
            return true;
        }
        if constexpr (std::equality_comparable<typename Registry::EntryType>)
        {
            return entry == other.entry;
        }
        else
        {
            return false;
        }
    }
};

template <typename Registry>
struct DefaultRegistryNESTypeClass
{
    bool operator==(const DefaultRegistryNESTypeClass&) const = default;

    [[nodiscard]] bool validate(const NESType& type) const
    {
        return type.tryGetAs<DefaultRegistryNESType<Registry>>() != nullptr;
    }
};

template <typename Registry>
struct DefaultNullaryRegistryFunctor
{
    std::shared_ptr<const int> identityToken;
    typename Registry::EntryType entry;
    NESType cachedType;

    [[nodiscard]] const typename Registry::EntryType& getRegistryEntry() const noexcept { return entry; }

    [[nodiscard]] static ExpectedNESTypeArguments getExpectedArguments() { return ExpectedNESTypeArguments{}; }

    [[nodiscard]] std::expected<NESType, Exception> instantiate(const NESTypeParameters& params) const
    {
        if (!params.empty())
        {
            return std::unexpected{
                TypeInferenceException("Nullary functor expects 0 type arguments, but got {}", params.size())};
        }
        return cachedType;
    }

    bool operator==(const DefaultNullaryRegistryFunctor& other) const
    {
        if (identityToken == other.identityToken)
        {
            return true;
        }
        if constexpr (std::equality_comparable<typename Registry::EntryType>)
        {
            return entry == other.entry;
        }
        else
        {
            return false;
        }
    }
};

template <typename Registry>
NamedNESParametrizedTypeClass makeDefaultRegistryTypeClass(std::string_view typeClassName)
{
    return NamedNESParametrizedTypeClass::constant(
        QualifiedIdentifier::parse(typeClassName),
        NESTypeClass{DefaultRegistryNESTypeClass<Registry>{}});
}

template <typename Registry>
NESFunctor makeDefaultNullaryRegistryFunctor(typename Registry::EntryType entry)
{
    auto token = std::make_shared<const int>(0);
    NESType cachedType{DefaultRegistryNESType<Registry>{.identityToken = token, .entry = entry}};
    return NESFunctor{DefaultNullaryRegistryFunctor<Registry>{
        .identityToken = std::move(token),
        .entry = std::move(entry),
        .cachedType = std::move(cachedType)}};
}

/// A registry a plugin makes available to the plugin system. Carries the registry's name,
/// singleton identity, optional exposed parameterized type classes, and a type-erased inserter.
struct RegistryProvision
{
    QualifiedIdentifier registryName;
    /// Identity of the singleton, for idempotent re-provision checks across shared libraries
    const void* singletonIdentity = nullptr;
    bool exposeToSqlScope = false;
    std::vector<NamedNESParametrizedTypeClass> typeClasses;
    std::function<InsertResult(const NamedNESFunctor&)> insertEntry;
};

/// An entry a plugin contributes to a registry. Pure data: the plugin system performs the actual
/// registration (and owns the duplicate/missing-registry/type policies, see PluginLoader).
struct EntryProvision
{
    NamedNESFunctor correspondingFunctor;
    QualifiedIdentifier registryName;
    /// Optional entries are skipped when the host does not know the registry; required entries
    /// fail the load instead.
    bool optional = false;
};

/// Everything a plugin provides to the plugin system, collected via BuiltinPlugins.
/// Plugins only describe; they never register anything themselves.
struct PluginDescriptor
{
    /// For diagnostics
    std::string name;
    std::vector<RegistryProvision> registries;
    std::vector<EntryProvision> entries;
};

/// The describe interface every plugin implements (generated by
/// cmake/RuntimeRegistrationUtil.cmake): plugins collect pointers to such functions in
/// BuiltinPlugins at static-initialization time.
using PluginDescribeFn = void (*)(PluginDescriptor&);

/// Creates the provision for a concrete registry; the inserter closes over the registry type,
/// which is what allows the plugin system to stay type-erased.
template <typename Registry>
RegistryProvision provideRegistry(std::string_view registryClassName)
{
    return RegistryProvision{
        .registryName = QualifiedIdentifier::parse(registryClassName),
        .singletonIdentity = &Registry::instance(),
        .exposeToSqlScope = Registry::EXPOSE_TO_SQL_SCOPE,
        .typeClasses = Registry::getParametrizedTypeClasses(),
        .insertEntry = [](const NamedNESFunctor& entry)
        {
            const auto* typedEntry = entry.getUnderlyingFunctor().template tryGetRegistryEntryAs<typename Registry::EntryType>();
            if (typedEntry == nullptr)
            {
                return InsertResult::EntryTypeMismatch;
            }
            const auto key = [&]() -> typename Registry::KeyType
            {
                if constexpr (std::same_as<typename Registry::KeyType, QualifiedIdentifier>)
                {
                    return entry.getFullyQualifiedName();
                }
                else
                {
                    /// Preserve original identifier casing for CaseSensitive registries (e.g. UnreflectionRegistry);
                    /// CaseSensitive=false registries normalize to uppercase inside RuntimeRegistry::internalKey.
                    return fmt::format(
                        "{}",
                        fmt::join(
                            entry.getFullyQualifiedName()
                                | std::views::transform([](const Identifier& part) { return part.getOriginalString(); }),
                            "."));
                }
            }();
            if (!Registry::instance().addEntry(key, *typedEntry))
            {
                return InsertResult::DuplicateKey;
            }
            if (!Registry::instance().addFunctor(key, entry))
            {
                return InsertResult::DuplicateKey;
            }
            return InsertResult::Success;
        }};
}

}
