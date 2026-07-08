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

#include <any>
#include <functional>
#include <memory>
#include <string>
#include <utility>

#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Util/RuntimeRegistry.hpp>
#include <InputFormatter.hpp>
#include <InputFormatterDescriptor.hpp>

namespace NES
{

/// Maps an input formatter type name to a factory that assembles an InputFormatter from a
/// descriptor and the memory provider of the pipeline. Entries self-register at static
/// initialization time via generated glue TUs
/// (see create_runtime_registry(InputFormatIndexer ...) in nes-input-formatters/CMakeLists.txt).
using InputFormatterFactoryFn
    = std::function<std::unique_ptr<InputFormatter>(const InputFormatterDescriptor&, std::shared_ptr<TupleBufferRef>)>;

class InputFormatIndexerRegistry
    : public RuntimeRegistry<InputFormatIndexerRegistry, std::string, InputFormatterFactoryFn, /*CaseSensitive*/ false>
{
};

/// Factory for the common case: the descriptor's type-erased config is the formatter's own config
/// struct (put there by this formatter's InputFormatterConfigRegistry entry), so the any_cast is
/// safe. We discourage keeping state in an indexer implementation, since the InputFormatter uses
/// its indexer concurrently — the indexer only receives its config and the buffer layout at
/// creation time, not the memory provider.
template <typename IndexerImpl, typename ConfigStruct>
InputFormatterFactoryFn makeInputFormatterFactory()
{
    return [](const InputFormatterDescriptor& descriptor, std::shared_ptr<TupleBufferRef> memoryProvider)
    {
        const auto& config = std::any_cast<const ConfigStruct&>(descriptor.getConfig());
        auto indexer = IndexerImpl::create(config, *memoryProvider);
        return std::make_unique<InputFormatter>(std::move(indexer), std::move(memoryProvider));
    };
}

}
