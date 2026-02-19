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

#include <InputFormatterTupleBufferRefProvider.hpp>

#include <memory>
#include <unordered_set>
#include <utility>

#include <DataTypes/Schema.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <ErrorHandling.hpp>
#include <InputFormatIndexerRegistry.hpp>

namespace NES
{

std::shared_ptr<InputFormatterTupleBufferRef> provideInputFormatterTupleBufferRef(
    ParserConfig formatScanConfig,
    std::shared_ptr<TupleBufferRef> memoryProvider,
    std::unordered_set<Record::RecordFieldIdentifier> fieldsToParse)
{
    if (auto inputFormatter = InputFormatIndexerRegistry::instance().create(
            formatScanConfig.parserType, InputFormatIndexerRegistryArguments(formatScanConfig, std::move(memoryProvider), fieldsToParse)))
    {
        return std::move(inputFormatter.value());
    }
    throw UnknownParserType("unknown type of input formatter: {}", formatScanConfig.parserType);
}

bool contains(const std::string& parserType)
{
    return InputFormatIndexerRegistry::instance().contains(parserType);
}
}
