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
#ifndef INCLUDED_FROM_SINK_REGISTRY
#    error "This file should not be included directly! Include instead include SinkRegistry.hpp"
#endif

#include <memory>
#include <string>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>

namespace NES::Sinks::SinkGeneratedRegistrar
{

std::unique_ptr<Sink> RegisterSinkPrint(QueryId, const SinkDescriptor&);
std::unique_ptr<Sink> RegisterSinkFile(QueryId, const SinkDescriptor&);

}

namespace NES
{
template <>
inline void Registrar<Sinks::SinkRegistry, Sinks::SinkRegistrySignature>::registerAll([[maybe_unused]] Registry<Registrar>& registry)
{
    using namespace NES::Sinks::SinkGeneratedRegistrar;
    registry.registerPlugin("Print", RegisterSinkPrint);
    registry.registerPlugin("File", RegisterSinkFile);
}

}
