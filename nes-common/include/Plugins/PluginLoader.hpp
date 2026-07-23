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

#include <vector>
#include <Plugins/PluginDescriptor.hpp>

namespace NES
{

/// The system side of plugin registration: consumes PluginDescriptors and performs the actual
/// work. Plugins never register anything themselves — they only describe registries and entries
/// (see PluginDescriptor) — so all registration policy lives here, exactly once: duplicate
/// registry names, duplicate entry keys, entry-type mismatches, and the optional-entry rule
/// (an entry for a registry unknown to this host is skipped if optional, fails the load if not).
class PluginLoader
{
public:
    /// Registers everything the given plugins provide, in two passes: the registries of ALL
    /// descriptors are added to the RegistryCatalog before the entries of ANY descriptor are
    /// registered, so entries can target registries provided by other plugins independent of
    /// order. Re-providing the same registry singleton under the same name is an idempotent
    /// no-op. Throws CannotLoadPlugin on registry name collisions, duplicate entry keys,
    /// entry-type mismatches, or required entries whose registry is unknown. There is no
    /// rollback: entries registered before a failure remain, so treat a failed call as fatal.
    static void registerPlugins(const std::vector<PluginDescriptor>& descriptors);
};

}
