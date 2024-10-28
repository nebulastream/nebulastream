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

#include <Sinks/SinkRegistry.hpp>

namespace NES::Sinks
{

/// Auto generated, injects all plugins into the SinkRegistry
class SinkGeneratedRegistrar
{
public:
    SinkGeneratedRegistrar() = default;
    ~SinkGeneratedRegistrar() = default;

    static void registerAllPlugins(SinkRegistry& registry)
    {
        /// External register functions
        /// no external plugins yet

        /// Internal register functions
        RegisterSinkPrint(registry);
        RegisterSinkFile(registry);
    }

private:
    /// External Registry Header Functions
    /// no external plugins yet

    /// Internal Registry Header Functions
    static void RegisterSinkPrint(SinkRegistry& registry);
    static void RegisterSinkFile(SinkRegistry& registry);
};

}
