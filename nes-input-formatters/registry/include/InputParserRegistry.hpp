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
#include <Util/Registry.hpp>
#include <InputParser.hpp>

namespace NES
{

using InputParserRegistryReturnType = std::unique_ptr<InputParser>;

struct InputParserRegistryArguments
{
    bool quotedText;
    bool hasTrailingSpaces;
};

class InputParserRegistry
    : public BaseRegistry<InputParserRegistry, std::string, InputParserRegistryReturnType, InputParserRegistryArguments>
{
};

}

#define INCLUDED_FROM_INPUTPARSER_REGISTRY
#include <InputParserGeneratedRegistrar.inc>
#undef INCLUDED_FROM_INPUTPARSER_REGISTRY
