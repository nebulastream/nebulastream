/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <iostream>
#include <cstring>

#include <Mobility/Storage/FilterStorage.h>
#include <Mobility/Utils/HashUtils.h>

namespace NES {

FilterStorage::FilterStorage(SchemaPtr schema, uint32_t storageSize) : schema(std::move(schema)), storageSize(storageSize) {
    bufferManager = std::make_shared<NodeEngine::BufferManager>(1024, 1024);
}

void FilterStorage::add(uint64_t tupleHash) {
    if (storedHashes.size() == storageSize) {
        storedHashes.erase(storedHashes.begin());
    }
    storedHashes.push_back(tupleHash);
}

bool FilterStorage::contains(uint64_t tupleHash) {
    return std::find(storedHashes.begin(), storedHashes.end(), tupleHash) != storedHashes.end();
}

NodeEngine::TupleBuffer FilterStorage::filter(NodeEngine::TupleBuffer buffer) {
    auto* buf = buffer.getBuffer<unsigned char>();
    auto filteredBuf = *bufferManager->getBufferNoBlocking();
    auto* filtered_tuples = filteredBuf.getBuffer();

    uint32_t filterOffset = 0;

    for (uint32_t i = 0; i < buffer.getNumberOfTuples(); i ++) {
        uint32_t offset = i * schema->getSchemaSizeInBytes();
        uint64_t hashValue = HashUtils::djb2(buf + offset, schema->getSchemaSizeInBytes());

        if (!contains(hashValue)) {
            add(hashValue);
            std::memcpy(filtered_tuples + filterOffset, buf + offset, schema->getSchemaSizeInBytes());
            filterOffset += schema->getSchemaSizeInBytes();
        }
    }

    filteredBuf.setNumberOfTuples(filterOffset / schema->getSchemaSizeInBytes());
    return filteredBuf;
}

uint32_t FilterStorage::size() { return storedHashes.size(); }

}
