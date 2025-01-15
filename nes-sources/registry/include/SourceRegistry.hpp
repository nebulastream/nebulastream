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
#include <variant>

#include <Sources/AsyncSource.hpp>
#include <Sources/BlockingSource.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/PluginRegistry.hpp>

namespace NES::Sources
{

using SourceRegistryReturnType = std::variant<std::unique_ptr<BlockingSource>, std::unique_ptr<AsyncSource>>;
struct SourceRegistryArguments
{
    SourceDescriptor sourceDescriptor;
};

using SourceRegistrySignature = RegistrySignature<std::string, SourceRegistryReturnType, const SourceRegistryArguments&>;
class SourceRegistry : public BaseRegistry<SourceRegistry, SourceRegistrySignature>
{
};

}

#define INCLUDED_FROM_SOURCE_REGISTRY
#include <SourceGeneratedRegistrar.inc>
#undef INCLUDED_FROM_SOURCE_REGISTRY
