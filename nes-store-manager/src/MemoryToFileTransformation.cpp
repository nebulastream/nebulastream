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

#include <MemoryToFileTransformation.hpp>

#include <utility>
#include <Util/Logger/Logger.hpp>
#include <MemoryStore.hpp>
#include <Store.hpp>
#include <StoreTransformationRegistry.hpp>

namespace NES::StoreManager
{

void MemoryToFileTransformation::execute(Store& source, Store& dest)
{
    auto typedSource = source.getAs<MemoryStore>();
    auto& memStore = typedSource.getMutable();
    auto schema = memStore.getSchema();

    auto buffers = memStore.drain();
    NES_DEBUG("MemoryToFileTransformation: drained {} buffers from MemoryStore", buffers.size());
    for (auto& buffer : buffers)
    {
        NES_DEBUG("MemoryToFileTransformation: writing buffer with {} tuples", buffer.getNumberOfTuples());
        dest.write(std::move(buffer), schema);
    }
}

}

namespace NES
{
StoreTransformationRegistryReturnType
StoreTransformationGeneratedRegistrar::RegisterMemoryStore_to_FileStoreStoreTransformation(StoreTransformationRegistryArguments)
{
    return StoreManager::MemoryToFileTransformation{};
}
}
