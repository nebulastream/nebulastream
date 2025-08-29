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

#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>

namespace NES
{

class SamplePhysicalFunction : public AggregationPhysicalFunction
{
public:
    /// @param inputType data type of the input
    /// @param resultType data type of the result
    /// @param inputFunction function for accessing the input
    /// @param resultFieldIdentifier field name of the return record
    /// @param memProviderPagedVector MemoryProvider for the pagedVector to store incoming records
    /// @param sampleSize number of elements in the sample
    SamplePhysicalFunction(
        DataType inputType,
        DataType resultType,
        PhysicalFunction inputFunction,
        Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
        std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider> memProviderPagedVector,
        uint64_t sampleSize);

    /// Returns the size of a record including size of variable sized data fields
    nautilus::val<uint64_t> getRecordDataSize(const Record& record) const;

protected:
    std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider> memProviderPagedVector;
    nautilus::val<uint64_t> sampleSize;
};

}
