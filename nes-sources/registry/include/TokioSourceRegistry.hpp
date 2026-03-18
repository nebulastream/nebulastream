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

#include <memory>
#include <string>

#include <Sources/TokioSource.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Registry.hpp>

namespace NES
{

using TokioSourceRegistryReturnType = std::unique_ptr<TokioSource>;

struct TokioSourceRegistryArguments
{
    SourceDescriptor sourceDescriptor;
    OriginId originId;
    uint32_t inflightLimit;
    std::shared_ptr<AbstractBufferProvider> bufferProvider;
};

class TokioSourceRegistry final
    : public BaseRegistry<TokioSourceRegistry, std::string, TokioSourceRegistryReturnType, TokioSourceRegistryArguments>
{
};

}

#define INCLUDED_FROM_TOKIO_SOURCE_REGISTRY
#include <TokioSourceGeneratedRegistrar.inc>
#undef INCLUDED_FROM_TOKIO_SOURCE_REGISTRY
