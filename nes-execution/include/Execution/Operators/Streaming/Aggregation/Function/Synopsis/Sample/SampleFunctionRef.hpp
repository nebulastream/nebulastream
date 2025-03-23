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

#include <Execution/Operators/ExecutionContext.hpp>

namespace NES::Runtime::Execution::Aggregation::Synopsis
{

class SampleFunctionRef
{
public:
    /// @param arena reference to arena object
    /// @param schema schema that matches the records that are supposed to be stored
    /// @param sampleSize number of records in the sample
    /// @param sampleDataSize accumulated data size of all records that are supposed to be stored
    /// (var sized data must account for actual data size plus sizeof uint32_t)
    SampleFunctionRef(
        ArenaRef& arena,
        const std::shared_ptr<Schema>& schema,
        const nautilus::val<uint64_t>& sampleSize,
        const nautilus::val<uint64_t>& sampleDataSize);

    /// @param sample sample wrapped in VariableSizedData object
    /// @param schema schema that matches the records that are supposed to be stored
    SampleFunctionRef(const VariableSizedData& sample, const std::shared_ptr<Schema>& schema);

    /// Writes the record to the memory address currently pointed to by currWritePtr and
    /// increases it by the size of the record that was written
    void writeRecord(const Record& record);

    /// Reads a record from the memory address currently pointed to by currReadPtr and
    /// increases it by the size of the record that was read
    [[nodiscard]] Record readRecord();

    void setReadPtrToPos(const nautilus::val<uint64_t>& pos);
    [[nodiscard]] VarVal getSample() const;

private:
    nautilus::val<int8_t*> memArea;
    std::shared_ptr<Schema> schema;
    nautilus::val<uint64_t> sampleSize;
    nautilus::val<uint64_t> sampleDataSize;
    nautilus::val<int8_t*> currWritePtr;
    nautilus::val<int8_t*> currReadPtr;
};

}
