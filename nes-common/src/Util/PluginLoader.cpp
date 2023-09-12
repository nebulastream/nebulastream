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
#include <PathConfig.h>
#include <Util/Logger/Logger.hpp>
#include <Util/PluginLoader.hpp>
#include <dlfcn.h>
#include <filesystem>

namespace NES::Util {

void PluginLoader::loadDefaultPlugins() {
    if (std::filesystem::exists(NES_PLUGINS_BUILD_PATH)) {
        NES_INFO("Found nes-plugins in local dir");
        loadPlugins(NES_PLUGINS_BUILD_PATH);
    } else if (std::filesystem::exists(NES_PLUGINS_INSTALL_PATH)) {
        NES_INFO("Found nes-plugins in parent dir");
        loadPlugins(NES_PLUGINS_INSTALL_PATH);
    } else {
        NES_THROW_RUNTIME_ERROR("Plugin path could not be found");
    }
}

void PluginLoader::loadPlugins(const std::filesystem::path& dirPath) {
    try {
        for (const auto& entry : std::filesystem::directory_iterator(dirPath)) {
            if (entry.is_regular_file() && entry.path().extension() == ".so") {
                loadPlugin(entry.path().string());
            }
        }
    } catch (std::filesystem::filesystem_error& e) {
        std::cerr << "Error: " << e.what() << '\n';
    }
}

void PluginLoader::loadPlugin(const std::string_view& path) {
    NES_INFO("Load Plugin {}", path);
    void* plugin = dlopen(path.data(), RTLD_NOW | RTLD_GLOBAL);
    if (!plugin) {
        NES_FATAL_ERROR("Error during plugin loaded: {}", dlerror());
    }
    loadedPlugins.emplace_back(plugin);
}

LoadedPlugin::LoadedPlugin(void* handle) : handle(handle) {}

LoadedPlugin::~LoadedPlugin() {
    NES_DEBUG("~LoadedPlugin()");
    if (dlclose(handle) == 0) {
        NES_FATAL_ERROR("Error during plugin un-loaded: {}", dlerror());
    }
}

}// namespace NES::Util