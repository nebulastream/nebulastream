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

#include <memory>


#include <InputFormatters/ScanProvider.hpp>

#include <memory>
#include <optional>
#include <string>
#include <utility>

#include <InputFormatters/RawScanPhysicalOperator.hpp>
#include <InputFormatters/ScanPhysicalOperator.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Sources/SourceDescriptor.hpp>

#include <Util/Strings.hpp>
#include <CSVTupleBufferMemoryProvider.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatIndexerRegistry.hpp>
#include <JSONTupleBufferMemoryProvider.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

PhysicalOperator provideScan(
    const std::optional<ParserConfig>& rawScanConfig, std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider> memoryProvider)
{
    if (rawScanConfig.has_value())
    {
        if (NES::Util::toUpperCase(rawScanConfig->parserType) == "CSV")
        {
            return ScanPhysicalOperator(std::make_shared<Interface::MemoryProvider::CSVTupleBufferMemoryProvider>(
                *rawScanConfig, memoryProvider->getMemoryLayout()));
        }
        else if (NES::Util::toUpperCase(rawScanConfig->parserType) == "JSON")
        {
            return ScanPhysicalOperator(std::make_shared<Interface::MemoryProvider::JSONTupleBufferMemoryProvider>(
                *rawScanConfig, memoryProvider->getMemoryLayout()));
        }
        throw UnknownParserType("unknown type of input formatter: {}", rawScanConfig.value().parserType);
    }
    return ScanPhysicalOperator(std::move(memoryProvider));
}

bool contains(const std::string& parserType)
{
    return InputFormatIndexerRegistry::instance().contains(parserType);
}
}
