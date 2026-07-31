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

#include <Util/Plugin.hpp>

#include <algorithm>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <vector>
#include <dlfcn.h>

namespace NES
{
namespace
{
using DynamicPluginInit = void (*)();

void loadDynamicPlugin(const std::filesystem::path& path)
{
    void* handle = dlopen(path.c_str(), RTLD_NOW | RTLD_LOCAL);
    if (handle == nullptr)
    {
        throw std::runtime_error("Could not load plugin " + path.string() + ": " + dlerror());
    }

    dlerror();
    auto init = reinterpret_cast<DynamicPluginInit>(dlsym(handle, "nes_plugin_init"));
    if (dlerror() != nullptr)
    {
        dlclose(handle);
        return;
    }

    /// Keep the library loaded because registries retain functions whose code lives in it.
    static std::vector<void*> handles;
    handles.push_back(handle);
    init();
}
}

void initializePlugins()
{
    static const bool initialized = []
    {
        for (auto plugin = __start_nes_plugin_init; plugin != __stop_nes_plugin_init; ++plugin)
        {
            plugin->init();
        }

        std::vector<std::filesystem::path> dynamicPlugins;
        for (const auto& entry : std::filesystem::directory_iterator(std::filesystem::current_path()))
        {
            if (entry.path().extension() == ".so")
            {
                dynamicPlugins.push_back(entry.path());
            }
        }
        std::ranges::sort(dynamicPlugins);
        for (const auto& path : dynamicPlugins)
        {
            loadDynamicPlugin(path);
        }
        return true;
    }();
    static_cast<void>(initialized);
}

}
