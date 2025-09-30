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
#include <optional>
#include <string>

#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

PhysicalOperator provideScan(
    const std::optional<ParserConfig>& rawScanConfig, std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider> memoryProvider);

bool contains(const std::string& parserType);
}
