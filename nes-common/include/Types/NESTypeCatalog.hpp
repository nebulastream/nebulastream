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

#include <optional>
#include <unordered_map>
#include <vector>

#include <Identifiers/QualifiedIdentifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Types/NESType.hpp>

namespace NES
{

/// Process-wide catalog of registered `NamedNESParametrizedTypeClass`es and `NamedNESFunctor`s,
/// populated by `PluginLoader::registerPlugins` when `loadBuiltinPlugins()` runs at startup.
/// Also provides scoped registration helpers for dynamic/test-only functors and type classes.
class NESTypeCatalog
{
    std::unordered_map<QualifiedIdentifier, NamedNESParametrizedTypeClass> typeClasses;
    std::unordered_map<QualifiedIdentifier, std::unordered_map<QualifiedIdentifier, NamedNESFunctor>> functorsByRegistry;
    std::unordered_map<QualifiedIdentifier, NamedNESFunctor> sqlExposedFunctors;

    NESTypeCatalog() = default;

public:
    NESTypeCatalog(const NESTypeCatalog&) = delete;
    NESTypeCatalog& operator=(const NESTypeCatalog&) = delete;
    NESTypeCatalog(NESTypeCatalog&&) = delete;
    NESTypeCatalog& operator=(NESTypeCatalog&&) = delete;
    ~NESTypeCatalog() = default;

    /// Out-of-line singleton so shared-library plugins share a single process-wide catalog.
    static NESTypeCatalog& instance();

    /// Registers a parameterized type class. Returns false if a different type class is already registered under the same name.
    [[nodiscard]] bool registerTypeClass(const NamedNESParametrizedTypeClass& typeClass);

    /// Registers a functor under `registryName` (and into the collision-free SQL scope if `exposeToSqlScope` is true).
    [[nodiscard]] bool
    registerFunctor(const QualifiedIdentifier& registryName, const NamedNESFunctor& functor, bool exposeToSqlScope);

    /// Removes a dynamically/test-registered functor by name (useful in unit tests).
    bool unregisterSqlFunctor(const QualifiedIdentifier& functorName);
    bool unregisterFunctor(const QualifiedIdentifier& registryName, const QualifiedIdentifier& functorName);
    bool unregisterTypeClass(const QualifiedIdentifier& typeClassName);

    [[nodiscard]] std::optional<NamedNESParametrizedTypeClass> findTypeClass(const QualifiedIdentifier& name) const;
    [[nodiscard]] std::vector<NamedNESParametrizedTypeClass> getAllTypeClasses() const;

    [[nodiscard]] std::optional<NamedNESFunctor>
    findFunctorInRegistry(const QualifiedIdentifier& registryName, const QualifiedIdentifier& functorName) const;
    [[nodiscard]] std::vector<NamedNESFunctor> getFunctorsInRegistry(const QualifiedIdentifier& registryName) const;

    [[nodiscard]] std::optional<NamedNESFunctor> findSqlFunctor(const QualifiedIdentifier& functorName) const;
    [[nodiscard]] std::vector<NamedNESFunctor> getAllSqlFunctors() const;
    [[nodiscard]] Schema<NamedNESFunctor, Unordered> getSqlFunctorSchema() const;
};

}
