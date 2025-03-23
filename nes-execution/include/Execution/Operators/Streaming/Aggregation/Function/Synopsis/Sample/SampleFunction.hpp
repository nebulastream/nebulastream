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

#include <Execution/Operators/Streaming/Aggregation/Function/AggregationFunction.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>

namespace NES::Runtime::Execution::Aggregation::Synopsis
{

class SampleFunction : public AggregationFunction
{
public:
    SampleFunction(
        std::shared_ptr<PhysicalType> inputType,
        std::shared_ptr<PhysicalType> resultType,
        std::unique_ptr<Functions::Function> inputFunction,
        Record::RecordFieldIdentifier resultFieldIdentifier,
        std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider> memProviderPagedVector,
        uint64_t sampleSize);

    /// Returns the size of a record including size of variable sized data fields
    nautilus::val<uint64_t> getRecordDataSize(const Record& record) const;

protected:
    std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider> memProviderPagedVector;
    nautilus::val<uint64_t> sampleSize;
    nautilus::val<uint64_t> sampleDataSize;
};

}
