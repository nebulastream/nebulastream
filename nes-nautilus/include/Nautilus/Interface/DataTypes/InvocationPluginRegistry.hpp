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
#include <Nautilus/Interface/DataTypes/InvocationPlugin.hpp>
#include <Nautilus/Interface/DataTypes/InvocationPluginRegistry.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Util/PluginRegistry.hpp>
#include <ErrorHandling.hpp>
namespace NES::Nautilus
{

class InvocationPluginRegistry : public BaseRegistry<InvocationPluginRegistry, std::string, InvocationPlugin>
{
public:
    const std::vector<std::unique_ptr<InvocationPlugin>>& getPlugins()
    {
        std::scoped_lock lock(mutex);
        if (plugins.empty())
        {
            for (const auto& registeredName : getRegisteredNames())
            {
                if (auto invocationPlugin = create(registeredName))
                    plugins.emplace_back(std::move(invocationPlugin.value()));
                else
                    throw UnknownInvocationType(fmt::format("Invocation plugin of type: {} not registered.", registeredName));
            }
        }
        return plugins;
    }

private:
    std::mutex mutex;
    std::vector<std::unique_ptr<InvocationPlugin>> plugins;
};

}

#define INCLUDED_FROM_INVOCATION_PLUGIN_REGISTRY
#include <Nautilus/Interface/DataTypes/GeneratedInvocationPluginRegistrar.hpp>
#undef INCLUDED_FROM_INVOCATION_PLUGIN_REGISTRY
