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
#include <string>
#include <Pipelines/ExecutablePipelineProvider.hpp>
#include <Util/Registry.hpp>

namespace NES
{
/// TODO #323: !!!IMPORTANT!!! This is not how we should use registries. We have an open issue to address this problem
using ExecutablePipelineProviderRegistryReturnType = std::unique_ptr<ExecutablePipelineProvider>;
struct ExecutablePipelineProviderRegistryArguments
{
};

class ExecutablePipelineProviderRegistry : public BaseRegistry<
                                               ExecutablePipelineProviderRegistry,
                                               std::string,
                                               ExecutablePipelineProviderRegistryReturnType,
                                               ExecutablePipelineProviderRegistryArguments>
{
};
}

#define INCLUDED_FROM_EXECUTABLE_PIPELINE_PROVIDER_REGISTRY
#include <ExecutablePipelineProviderGeneratedRegistrar.inc>
#undef INCLUDED_FROM_EXECUTABLE_PIPELINE_PROVIDER_REGISTRY