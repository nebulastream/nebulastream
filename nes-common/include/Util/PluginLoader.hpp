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

#ifndef NES_COMMON_INCLUDE_UTIL_PLUGINLOADER_HPP_
#define NES_COMMON_INCLUDE_UTIL_PLUGINLOADER_HPP_
#include <filesystem>
#include <list>
#include <string_view>
namespace NES::Util {

class LoadedPlugin {
  public:
    LoadedPlugin(void* handle);
    ~LoadedPlugin();

  private:
    void* handle;
};

class PluginLoader {

  public:
    void loadDefaultPlugins();
    void loadPlugins(const std::filesystem::path& dirPath);
    void loadPlugin(const std::string_view& path);

  private:
    std::list<LoadedPlugin> loadedPlugins;
};

}// namespace NES::Util

#endif  // NES_COMMON_INCLUDE_UTIL_PLUGINLOADER_HPP_
