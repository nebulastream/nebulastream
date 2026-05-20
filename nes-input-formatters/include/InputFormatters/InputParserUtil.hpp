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

#include <InputParser.hpp>

namespace NES
{

/// Current placeholder for a fully functional InputParserDescriptor.
/// Attributes are either passed into the parser implementation or used in the InputParserProvider to determine the parser type.
struct InputParserConfig
{
    bool nullable;
    bool quotedText;
    bool hasTrailingSpace;
};

/// Fetches InputParser from Registry
/// The concrete type of parser is [Nullable]parserTypeInputParser
std::unique_ptr<InputParser> provideInputParser(const std::string& parserType, const InputParserConfig& config);
}
