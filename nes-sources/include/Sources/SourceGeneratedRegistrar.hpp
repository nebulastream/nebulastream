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
#ifndef INCLUDED_FROM_SOURCE_REGISTRY
#    error "This file should not be included directly! Include instead include SourceRegistry.hpp"
#endif

#include <memory>
#include <string>
#include <API/Schema.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/PluginRegistry.hpp>

namespace NES::Sources::SourceGeneratedRegistrar
{

std::unique_ptr<Source> RegisterSourceCSV(const Schema&, const SourceDescriptor&);
std::unique_ptr<Source> RegisterSourceTCP(const Schema&, const SourceDescriptor&);

}

namespace NES
{
template <>
inline void Registrar<std::string, NES::Sources::Source, const Schema&, const Sources::SourceDescriptor&>::registerAll(
    [[maybe_unused]] Registry<Registrar>& registry)
{
    using namespace NES::Sources::SourceGeneratedRegistrar;
    registry.registerPlugin("CSV", RegisterSourceCSV);
    registry.registerPlugin("TCP", RegisterSourceTCP);
}
}
