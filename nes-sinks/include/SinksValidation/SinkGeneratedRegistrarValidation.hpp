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

#include <SinksValidation/SinkGeneratedRegistrarValidation.hpp>
#include <SinksValidation/SinkRegistryValidation.hpp>

namespace NES::Sinks
{

/// Auto generated, injects all plugins into the RegistrySink
class SinkGeneratedRegistrarValidation
{
public:
    SinkGeneratedRegistrarValidation() = default;
    ~SinkGeneratedRegistrarValidation() = default;

    static void registerAllPlugins(SinkRegistryValidation& registry)
    {
        /// External register functions
        /// no external plugins yet

        /// Internal register functions
        RegisterSinkValidationPrint(registry);
        RegisterSinkValidationFile(registry);
    }

private:
    /// External Registry Header Functions
    /// no external plugins yet

    /// Internal Registry Header Functions
    static void RegisterSinkValidationPrint(SinkRegistryValidation& registry);
    static void RegisterSinkValidationFile(SinkRegistryValidation& registry);
};

}
