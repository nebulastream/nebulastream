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

#include <Plugins/PluginLoader.hpp>

#include <vector>
#include <Plugins/PluginDescriptor.hpp>
#include <Plugins/RegistryCatalog.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
void announceRegistries(const PluginDescriptor& descriptor)
{
    for (const auto& provision : descriptor.registries)
    {
        if (const auto existing = RegistryCatalog::instance().find(provision.registryClassName))
        {
            if (existing->instanceAddress == provision.instanceAddress)
            {
                continue;
            }
            throw CannotLoadPlugin(
                "{} provides registry {}, but a different registry is already announced under that name",
                descriptor.name,
                provision.registryClassName);
        }
        if (!RegistryCatalog::instance().addEntry(provision.registryClassName, provision))
        {
            throw CannotLoadPlugin("{} failed to announce registry {}", descriptor.name, provision.registryClassName);
        }
    }
}

void registerEntries(const PluginDescriptor& descriptor)
{
    for (const auto& entry : descriptor.entries)
    {
        const auto provision = RegistryCatalog::instance().find(entry.registryClassName);
        if (!provision)
        {
            if (entry.optional)
            {
                NES_DEBUG(
                    "Skipping optional entry \"{}\": registry {} is not available in this host ({})",
                    entry.key,
                    entry.registryClassName,
                    descriptor.name);
                continue;
            }
            throw CannotLoadPlugin(
                "{} requires registry {} for entry \"{}\", but it is not available in this host",
                descriptor.name,
                entry.registryClassName,
                entry.key);
        }
        switch (provision->insertEntry(entry.key, entry.entry))
        {
            case InsertResult::Success:
                break;
            case InsertResult::DuplicateKey:
                throw CannotLoadPlugin("{}: duplicate registration for \"{}\" in {}", descriptor.name, entry.key, entry.registryClassName);
            case InsertResult::EntryTypeMismatch:
                throw CannotLoadPlugin(
                    "{}: entry \"{}\" does not match the entry type of {}", descriptor.name, entry.key, entry.registryClassName);
        }
    }
}
}

void PluginLoader::registerPlugins(const std::vector<PluginDescriptor>& descriptors)
{
    /// All registries before any entries, so entries can target registries provided by other
    /// plugins independent of order.
    for (const auto& descriptor : descriptors)
    {
        announceRegistries(descriptor);
    }
    for (const auto& descriptor : descriptors)
    {
        registerEntries(descriptor);
    }
}

}
