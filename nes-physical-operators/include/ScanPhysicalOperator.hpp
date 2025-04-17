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

#include <memory>
#include <vector>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <AbstractPhysicalOperator.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>

namespace NES
{

/// @brief This basic scan operator extracts records from a base tuple buffer according to a memory layout.
/// Furthermore, it supports projection push down to eliminate unneeded reads
class ScanPhysicalOperator : public AbstractPhysicalOperator
{
public:
    /// @brief Constructor for the scan operator that receives a memory layout and a projection vector.
    /// @param memoryLayout memory layout that describes the tuple buffer.
    /// @param projections projection vector
    ScanPhysicalOperator(
        std::unique_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider> memoryProvider,
        std::vector<Nautilus::Record::RecordFieldIdentifier> projections);

    void open(ExecutionContext& executionCtx, Nautilus::RecordBuffer& recordBuffer) const override;

    std::string toString() const {return typeid(this).name(); }

private:
    std::unique_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider> memoryProvider;
    std::vector<Nautilus::Record::RecordFieldIdentifier> projections;
};

}
