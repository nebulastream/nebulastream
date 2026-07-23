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

#include <Plugins/BuiltinPlugins.hpp>

#include <mutex>
#include <Plugins/PluginDescriptor.hpp>
#include <Plugins/PluginLoader.hpp>

namespace NES
{

BuiltinPlugins& BuiltinPlugins::instance()
{
    static BuiltinPlugins inst;
    return inst;
}

PluginDescriptor BuiltinPlugins::describe() const
{
    PluginDescriptor descriptor{.name = "builtin", .registries = {}, .entries = {}};
    for (const auto& describer : describers)
    {
        describer(descriptor);
    }
    return descriptor;
}

void loadBuiltinPlugins()
{
    static std::once_flag loaded;
    /// call_once: on exception the flag stays unset, so a failed startup can be retried.
    std::call_once(loaded, [] { PluginLoader::registerPlugins({BuiltinPlugins::instance().describe()}); });
}

}
