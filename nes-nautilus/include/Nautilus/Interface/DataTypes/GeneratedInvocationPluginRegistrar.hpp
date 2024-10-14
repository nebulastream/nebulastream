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

#ifndef INCLUDED_FROM_INVOCATION_PLUGIN_REGISTRY
#    error "This file should not be included directly! " \
"Include instead include <Natuilus/Interface/DataTypes/InvocationPluginRegistry.hpp>"
#endif

namespace NES::Nautilus
{

/// TODO #310: Allow cmake to generate weak function calls, which may or may not be resolved during linking.
///       If the function was not found during linking the function will be null, but no link error is raised.
///       During runtime we can check if the function symbol is not null before calling the function.
///       The Nautilus Test adds a CustomType and provides an InvocationPlugin, this type and plugin are only
///       used in the test.
std::unique_ptr<InvocationPlugin> __attribute__((weak)) RegisterCustomTypeInvocationPlugin();

std::unique_ptr<InvocationPlugin> RegisterBooleanInvocationPlugin();
std::unique_ptr<InvocationPlugin> RegisterFloatInvocationPlugin();
std::unique_ptr<InvocationPlugin> RegisterIdentifierInvocationPlugin();
std::unique_ptr<InvocationPlugin> RegisterIntInvocationPlugin();
std::unique_ptr<InvocationPlugin> RegisterListInvocationPlugin();
std::unique_ptr<InvocationPlugin> RegisterMemRefInvocationPlugin();
std::unique_ptr<InvocationPlugin> RegisterTextInvocationPlugin();
std::unique_ptr<InvocationPlugin> RegisterTimeStampInvocationPlugin();

}
namespace NES
{
template <>
inline void Registrar<std::string, Nautilus::InvocationPlugin>::registerAll([[maybe_unused]] Registry<Registrar>& registry)
{
    using namespace NES::Nautilus;
    if (RegisterCustomTypeInvocationPlugin)
    {
        registry.registerPlugin("custom", RegisterCustomTypeInvocationPlugin);
    }
    registry.registerPlugin("boolean", RegisterBooleanInvocationPlugin);
    registry.registerPlugin("float", RegisterFloatInvocationPlugin);
    registry.registerPlugin("identifier", RegisterIdentifierInvocationPlugin);
    registry.registerPlugin("int", RegisterIntInvocationPlugin);
    registry.registerPlugin("list", RegisterListInvocationPlugin);
    registry.registerPlugin("memRef", RegisterMemRefInvocationPlugin);
    registry.registerPlugin("text", RegisterTextInvocationPlugin);
    registry.registerPlugin("timeStamp", RegisterTimeStampInvocationPlugin);
}

}
