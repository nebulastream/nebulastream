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
#include <ChainedHashMapTestUtils.hpp>

namespace NES::Nautilus::TestUtils
{
class ChainedHashMapCustomValueTestUtils : public TestUtils::ChainedHashMapTestUtils
{
public:
    /// Compiles a method that finds a key in the hash map and inserts all record from the value buffer into the paged vector
    /// We are using the findOrCreateEntry() method of the hash map interface.
    nautilus::engine::
        CallableFunction<void, Memory::TupleBuffer*, Memory::TupleBuffer*, uint64_t, Memory::AbstractBufferProvider*, Interface::HashMap*>
        compileFindAndInsertIntoPagedVector(const std::vector<Record::RecordFieldIdentifier>& projectionAllFields) const;


    /// Compiles a function that iterates over all keys and writes all values for one key to the output buffer.
    /// We use the findOrCreateEntry() method of the hash map interface.
    nautilus::engine::
        CallableFunction<void, Memory::TupleBuffer*, uint64_t, Memory::TupleBuffer*, Memory::AbstractBufferProvider*, Interface::HashMap*>
        compileWriteAllRecordsIntoOutputBuffer(const std::vector<Record::RecordFieldIdentifier>& projectionAllFields) const;
};
}
