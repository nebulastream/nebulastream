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

#include <InputFormatters/InputFormatter.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Registry.hpp>

namespace NES::InputFormatters
{

using InputFormatterRegistryReturnType = std::unique_ptr<InputFormatter>;
/// A InputFormatter requires a schema, a tuple separator and a field delimiter.
struct InputFormatterRegistryArguments
{
    Sources::ParserConfig inputFormatterConfig;
    size_t numberOfFieldsInSchema;
};

class InputFormatterRegistry
    : public BaseRegistry<InputFormatterRegistry, std::string, InputFormatterRegistryReturnType, InputFormatterRegistryArguments>
{
};

}

#define INCLUDED_FROM_SOURCE_PARSER_REGISTRY
#include <InputFormatterGeneratedRegistrar.inc>
#undef INCLUDED_FROM_SOURCE_PARSER_REGISTRY