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

/// NOLINT(clang-diagnostic-error)
#ifndef INCLUDED_FROM_SOURCE_PARSER_REGISTRY
#    error "This file should not be included directly! Include instead include SourceParserRegistry.hpp"
#endif

#include <memory>
#include <string>
#include <SourceParsers/SourceParser.hpp>
#include <Util/PluginRegistry.hpp>

namespace NES::SourceParsers::SourceParserGeneratedRegistrar
{

std::unique_ptr<SourceParser> RegisterSourceParserCSV(std::shared_ptr<Schema>, std::string, std::string);

}

namespace NES
{
template <>
inline void Registrar<SourceParsers::SourceParserRegistry, SourceParsers::SourceParserRegistrySignature>::registerAll(
    [[maybe_unused]] Registry<Registrar>& registry)
{
    using namespace NES::SourceParsers::SourceParserGeneratedRegistrar;
    registry.registerPlugin("CSV", RegisterSourceParserCSV);
}
}
