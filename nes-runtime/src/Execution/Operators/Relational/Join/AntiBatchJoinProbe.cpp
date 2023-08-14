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

#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Relational/Join/BatchJoinHandler.hpp>
#include <Execution/Operators/Relational/Join/AbstractBatchJoinProbe.hpp>
#include <Execution/Operators/Relational/Join/AntiBatchJoinProbe.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/DataTypes/MemRefUtils.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <utility>

namespace NES::Runtime::Execution::Operators {

AntiBatchJoinProbe::AntiBatchJoinProbe(uint64_t operatorHandlerIndex,
                               const std::vector<Expressions::ExpressionPtr>& keyExpressions,
                               const std::vector<PhysicalTypePtr>& keyDataTypes,
                               const std::vector<Record::RecordFieldIdentifier>& probeFieldIdentifiers,
                               const std::vector<PhysicalTypePtr>& valueDataTypes,
                               std::unique_ptr<Nautilus::Interface::HashFunction> hashFunction)
    : AbstractBatchJoinProbe(operatorHandlerIndex, keyExpressions, keyDataTypes, probeFieldIdentifiers, valueDataTypes, std::move(hashFunction)) {}

void AntiBatchJoinProbe::execute(NES::Runtime::Execution::ExecutionContext& ctx, NES::Nautilus::Record& record) const {
    // Get matches iterator
    auto entry = findMatches(ctx, record);

    // Emit if no match was found
    if (entry == nullptr) {
        // Emitted records could be duplicates, thus we need a distinct operator afterward
        this->child->execute(ctx, record);
    }
}

}// namespace NES::Runtime::Execution::Operators