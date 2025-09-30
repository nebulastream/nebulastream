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

#include <RawScanProvider.hpp>

#include <memory>
#include <utility>

#include <DataTypes/Schema.hpp>
#include <RawScanPhysicalOperator.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <ErrorHandling.hpp>
#include <RawScanIndexerRegistry.hpp>

namespace NES
{

PhysicalOperator
provideRawScan(const ParserConfig& rawScanConfig, std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider> memoryProvider)
{
    if (auto inputFormatter = RawScanIndexerRegistry::instance().create(
            rawScanConfig.parserType, RawScanIndexerRegistryArguments(rawScanConfig, std::move(memoryProvider))))
    {
        return RawScanPhysicalOperator(std::move(inputFormatter.value()));
    }
    throw UnknownParserType("unknown type of input formatter: {}", rawScanConfig.parserType);
}

bool containsRawScan(const std::string& parserType)
{
    return RawScanIndexerRegistry::instance().contains(parserType);
}
}
