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

#include <cstddef>
#include <memory>
#include <string>

#include <OutputFormatters/OutputFormatter.hpp>
#include <OutputFormatters/OutputFormatterDescriptor.hpp>
#include <Util/Registry.hpp>

namespace NES
{

using OutputFormatterRegistryReturnType = std::unique_ptr<OutputFormatter>;

struct OutputFormatterRegistryArguments
{
    size_t numberOfFields;
    OutputFormatterDescriptor descriptor;
};

class OutputFormatterRegistry
    : public BaseRegistry<OutputFormatterRegistry, std::string, OutputFormatterRegistryReturnType, OutputFormatterRegistryArguments>
{
};

}

#define INCLUDED_FROM_OUTPUTFORMATTER_REGISTRY
#include <OutputFormatterGeneratedRegistrar.inc>
#undef INCLUDED_FROM_OUTPUTFORMATTER_REGISTRY
