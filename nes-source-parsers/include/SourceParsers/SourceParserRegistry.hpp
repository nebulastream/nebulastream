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

#include <API/Schema.hpp>
#include <SourceParsers/SourceParser.hpp>
#include <Util/PluginRegistry.hpp>

namespace NES::SourceParsers
{

/// A SourceParser requires a schema, a tuple separator and a field delimiter.
using SourceParserRegistrySignature = RegistrySignature<std::string, SourceParser, std::shared_ptr<Schema>, std::string, std::string>;
class SourceParserRegistry : public BaseRegistry<SourceParserRegistry, SourceParserRegistrySignature>
{
};

}

#define INCLUDED_FROM_SOURCE_PARSER_REGISTRY
#include <SourceParsers/SourceParserGeneratedRegistrar.hpp>
#undef INCLUDED_FROM_SOURCE_PARSER_REGISTRY
