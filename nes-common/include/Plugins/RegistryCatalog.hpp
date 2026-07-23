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

#include <string>
#include <Plugins/PluginDescriptor.hpp>
#include <Util/RuntimeRegistry.hpp>

namespace NES
{

/// Meta-registry of all runtime registries in the process ("registry of registries"), keyed by
/// the registry class name (e.g. "SourceRegistry"). Populated by the PluginLoader from the
/// RegistryProvisions that plugins (built-in and dynamic) describe. Entries are registered
/// through the stored provisions' type-erased inserters, which keeps optional cross-plugin
/// registrations a plain lookup miss instead of an unresolved-symbol load failure.
class RegistryCatalog : public RuntimeRegistry<RegistryCatalog, std::string, RegistryProvision>
{
public:
    /// Defined out-of-line (RegistryCatalog.cpp) instead of inheriting the base's inline
    /// definition, so exactly one instance exists process-wide even with plugins loaded as
    /// shared objects.
    static RegistryCatalog& instance();
};

}
