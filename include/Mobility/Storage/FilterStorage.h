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

#ifndef NES_FILTERSTORAGE_H
#define NES_FILTERSTORAGE_H

#include <set>
#include <API/Schema.hpp>
#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/TupleBuffer.hpp>

namespace NES {


class FilterStorage {
  private:
    SchemaPtr schema;
    uint32_t storageSize;
    std::set<uint64_t> storedHashes;
    NodeEngine::BufferManagerPtr bufferManager;

    void add(uint64_t tupleHash);
    bool contains(uint64_t tupleHash);

  public:
    explicit FilterStorage(SchemaPtr  schema, uint32_t maxTuples);
    NodeEngine::TupleBuffer filter(NodeEngine::TupleBuffer buffer);
    uint32_t size();
};

}

#endif//NES_FILTERSTORAGE_H
