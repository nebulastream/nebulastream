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

#include <functional>
#include <memory>
#include <string>

#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/RuntimeRegistry.hpp>

namespace NES
{

using SourceRegistryReturnType = std::unique_ptr<Source>;

struct SourceRegistryArguments
{
    SourceDescriptor sourceDescriptor;
};

using SourceFactoryFn = std::function<SourceRegistryReturnType(SourceRegistryArguments)>;

/// Creates the registry entry for a source implementation. Sources are constructed from their
/// descriptor; the entry expression in cmake/RuntimeRegistrationUtil.cmake instantiates this per
/// plugin type.
template <typename SourceImpl>
SourceFactoryFn makeSourceFactory()
{
    return [](SourceRegistryArguments arguments) -> SourceRegistryReturnType
    { return std::make_unique<SourceImpl>(arguments.sourceDescriptor); };
}

/// Filled by loadBuiltinPlugins() / plugin registration (see cmake/RuntimeRegistrationUtil.cmake).
/// Case-insensitive to mirror the retired BaseRegistry: descriptors carry canonical upper-case
/// type names (e.g. "NETWORK") while plugins register under their spelled name (e.g. "Network").
class SourceRegistry : public RuntimeRegistry<SourceRegistry, std::string, SourceFactoryFn, /*CaseSensitive*/ false>
{
public:
    /// Defined out-of-line (SourceRegistry.cpp) instead of inheriting the base's inline definition, so
    /// exactly one instance exists process-wide even when plugins are loaded as shared objects.
    static SourceRegistry& instance();
};

}
