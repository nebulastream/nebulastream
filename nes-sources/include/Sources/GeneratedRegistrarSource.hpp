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

#include <Sources/RegistrySource.hpp>

namespace NES::Sources
{

/// Auto generated, injects all plugins into the RegistrySource
class GeneratedRegistrarSource
{
public:
    GeneratedRegistrarSource() = default;
    ~GeneratedRegistrarSource() = default;

    static void registerAllPlugins(RegistrySource& registry)
    {
        /// External register functions
        /// no external plugins yet

        /// Internal register functions
        RegisterSourceTCP(registry);
        RegisterSourceCSV(registry);
    }

private:
    /// External Registry Header Functions
    /// no external plugins yet

    /// Internal Registry Header Functions
    static void RegisterSourceTCP(RegistrySource& registry);
    static void RegisterSourceCSV(RegistrySource& registry);
};

}
