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

#include <Plugins/PluginCatalog.hpp>

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <dlfcn.h>
#include <Plugins/BuiltinPlugins.hpp>
#include <Plugins/PluginAbi.hpp>
#include <Plugins/PluginLoader.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
template <typename EntryPointFn>
EntryPointFn resolveEntryPoint(void* handle, const std::filesystem::path& path, const char* symbol)
{
    const auto entryPoint = reinterpret_cast<EntryPointFn>(dlsym(handle, symbol));
    if (entryPoint == nullptr)
    {
        throw CannotLoadPlugin("{} does not export {}; not a NES plugin?", path.string(), symbol);
    }
    return entryPoint;
}
}

PluginCatalog::PluginCatalog()
{
    loadBuiltinPlugins();
}

void PluginCatalog::load(const std::vector<std::filesystem::path>& paths)
{
    const std::lock_guard lock{mutex};

    /// Phase 1: dlopen every not-yet-loaded shared object. Passive — the generated plugin glue
    /// has no registration side effects at static initialization time.
    std::vector<LoadedPlugin> pending;
    for (const auto& path : paths)
    {
        auto canonicalPath = std::filesystem::weakly_canonical(path);
        const auto sameAs = [&canonicalPath](const LoadedPlugin& other) { return other.path == canonicalPath; };
        if (std::ranges::any_of(plugins, sameAs) or std::ranges::any_of(pending, sameAs))
        {
            NES_INFO("Plugin {} is already loaded; skipping", canonicalPath.string());
            continue;
        }
        void* handle = dlopen(canonicalPath.c_str(), RTLD_NOW | RTLD_LOCAL);
        if (handle == nullptr)
        {
            throw CannotLoadPlugin("dlopen failed for {}: {}", canonicalPath.string(), dlerror());
        }
        pending.emplace_back(std::move(canonicalPath), handle);
    }

    /// Phase 2: validate the ABI and collect every plugin's descriptor. Plugins only describe
    /// themselves; nothing is registered yet.
    std::vector<PluginDescriptor> descriptors;
    descriptors.reserve(pending.size());
    for (const auto& [path, handle] : pending)
    {
        const auto abiVersionFn = resolveEntryPoint<PluginAbiVersionFn>(handle, path, "nes_plugin_abi_version");
        if (const auto abiVersion = abiVersionFn(); abiVersion != PLUGIN_ABI_VERSION)
        {
            throw CannotLoadPlugin(
                "{} was built against plugin ABI version {}, host expects {}", path.string(), abiVersion, PLUGIN_ABI_VERSION);
        }
        auto& descriptor = descriptors.emplace_back();
        descriptor.name = path.string();
        resolveEntryPoint<PluginDescribeFn>(handle, path, "nes_plugin_describe")(descriptor);
    }

    /// Phase 3: the loader performs the registration; it orders the registries of all
    /// plugins before the entries of any plugin internally. If it throws, none of the pending
    /// plugins are recorded as loaded (entries registered before the failure remain — there is
    /// no rollback — so a failed load must be treated as fatal).
    PluginLoader::registerPlugins(descriptors);
    for (auto& plugin : pending)
    {
        NES_INFO("Loaded plugin {}", plugin.path.string());
        plugins.push_back(std::move(plugin));
    }
}

void PluginCatalog::loadFromEnvironment()
{
    const char* const pluginPaths = std::getenv("NES_PLUGINS");
    if (pluginPaths == nullptr or std::string_view{pluginPaths}.empty())
    {
        return;
    }
    std::vector<std::filesystem::path> paths;
    for (std::string_view remaining{pluginPaths}; not remaining.empty();)
    {
        const auto separator = remaining.find(':');
        if (const auto entry = remaining.substr(0, separator); not entry.empty())
        {
            paths.emplace_back(entry);
        }
        remaining = (separator == std::string_view::npos) ? std::string_view{} : remaining.substr(separator + 1);
    }
    load(paths);
}

std::vector<PluginCatalog::LoadedPlugin> PluginCatalog::loadedPlugins() const
{
    const std::lock_guard lock{mutex};
    return plugins;
}

}
