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
#include <cstdint>
#include <memory>
#include <vector>
#include <DataTypes/Schema.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <nautilus/Engine.hpp>

namespace NES::TestUtils
{


void runStoreTest(
    PagedVector& pagedVector,
    const Schema& testSchema,
    uint64_t pageSize,
    const std::vector<Record::RecordFieldIdentifier>& projections,
    const std::vector<TupleBuffer>& allRecords,
    const nautilus::engine::NautilusEngine& nautilusEngine,
    AbstractBufferProvider& bufferManager);

void runRetrieveTest(
    PagedVector& pagedVector,
    const Schema& testSchema,
    uint64_t pageSize,
    const std::vector<Record::RecordFieldIdentifier>& projections,
    const std::vector<TupleBuffer>& allRecords,
    const nautilus::engine::NautilusEngine& nautilusEngine,
    AbstractBufferProvider& bufferManager);

void insertAndAppendAllPagesTest(
    const std::vector<Record::RecordFieldIdentifier>& projections,
    const Schema& schema,
    uint64_t entrySize,
    uint64_t pageSize,
    const std::vector<std::vector<TupleBuffer>>& allRecordsAndVectors,
    const std::vector<TupleBuffer>& expectedRecordsAfterAppendAll,
    uint64_t differentPageSizes,
    const nautilus::engine::NautilusEngine& nautilusEngine,
    AbstractBufferProvider& bufferManager);

}
