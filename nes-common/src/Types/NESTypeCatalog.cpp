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

#include <Types/NESTypeCatalog.hpp>

#include <mutex>
#include <optional>
#include <ranges>
#include <shared_mutex>
#include <vector>

#include <Identifiers/QualifiedIdentifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Types/NESType.hpp>

namespace NES
{

NESTypeCatalog& NESTypeCatalog::instance()
{
    static NESTypeCatalog inst;
    return inst;
}

bool NESTypeCatalog::registerTypeClass(const NamedNESParametrizedTypeClass& typeClass)
{
    std::unique_lock lock{mutex};
    const auto& key = typeClass.getFullyQualifiedName();
    if (const auto it = typeClasses.find(key); it != typeClasses.end())
    {
        return it->second == typeClass;
    }
    typeClasses.emplace(key, typeClass);
    return true;
}

bool NESTypeCatalog::registerFunctor(
    const QualifiedIdentifier& registryName, const NamedNESFunctor& functor, const bool exposeToSqlScope)
{
    std::unique_lock lock{mutex};
    const auto& functorName = functor.getFullyQualifiedName();
    if (exposeToSqlScope && sqlExposedFunctors.contains(functorName))
    {
        return false;
    }

    auto& regMap = functorsByRegistry[registryName];
    if (!regMap.try_emplace(functorName, functor).second)
    {
        return false;
    }

    if (exposeToSqlScope)
    {
        sqlExposedFunctors.emplace(functorName, functor);
    }
    return true;
}

bool NESTypeCatalog::unregisterSqlFunctor(const QualifiedIdentifier& functorName)
{
    std::unique_lock lock{mutex};
    return sqlExposedFunctors.erase(functorName) > 0;
}

bool NESTypeCatalog::unregisterFunctor(const QualifiedIdentifier& registryName, const QualifiedIdentifier& functorName)
{
    std::unique_lock lock{mutex};
    sqlExposedFunctors.erase(functorName);
    if (const auto regIt = functorsByRegistry.find(registryName); regIt != functorsByRegistry.end())
    {
        return regIt->second.erase(functorName) > 0;
    }
    return false;
}

bool NESTypeCatalog::unregisterTypeClass(const QualifiedIdentifier& typeClassName)
{
    std::unique_lock lock{mutex};
    return typeClasses.erase(typeClassName) > 0;
}

std::optional<NamedNESParametrizedTypeClass> NESTypeCatalog::findTypeClass(const QualifiedIdentifier& name) const
{
    std::shared_lock lock{mutex};
    if (const auto it = typeClasses.find(name); it != typeClasses.end())
    {
        return it->second;
    }
    return std::nullopt;
}

std::vector<NamedNESParametrizedTypeClass> NESTypeCatalog::getAllTypeClasses() const
{
    std::shared_lock lock{mutex};
    return typeClasses | std::views::values | std::ranges::to<std::vector>();
}

std::optional<NamedNESFunctor>
NESTypeCatalog::findFunctorInRegistry(const QualifiedIdentifier& registryName, const QualifiedIdentifier& functorName) const
{
    std::shared_lock lock{mutex};
    if (const auto regIt = functorsByRegistry.find(registryName); regIt != functorsByRegistry.end())
    {
        if (const auto fnIt = regIt->second.find(functorName); fnIt != regIt->second.end())
        {
            return fnIt->second;
        }
    }
    return std::nullopt;
}

std::vector<NamedNESFunctor> NESTypeCatalog::getFunctorsInRegistry(const QualifiedIdentifier& registryName) const
{
    std::shared_lock lock{mutex};
    if (const auto regIt = functorsByRegistry.find(registryName); regIt != functorsByRegistry.end())
    {
        return regIt->second | std::views::values | std::ranges::to<std::vector>();
    }
    return {};
}

std::optional<NamedNESFunctor> NESTypeCatalog::findSqlFunctor(const QualifiedIdentifier& functorName) const
{
    std::shared_lock lock{mutex};
    if (const auto it = sqlExposedFunctors.find(functorName); it != sqlExposedFunctors.end())
    {
        return it->second;
    }
    return std::nullopt;
}

std::vector<NamedNESFunctor> NESTypeCatalog::getAllSqlFunctors() const
{
    std::shared_lock lock{mutex};
    return sqlExposedFunctors | std::views::values | std::ranges::to<std::vector>();
}

Schema<NamedNESFunctor, Unordered> NESTypeCatalog::getSqlFunctorSchema() const
{
    std::shared_lock lock{mutex};
    return sqlExposedFunctors | std::views::values | std::ranges::to<Schema<NamedNESFunctor, Unordered>>();
}

}
