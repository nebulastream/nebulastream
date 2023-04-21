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

#include <API/AttributeField.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/JoinPhases/NLJBuild.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>

namespace NES::Runtime::Execution::Operators {



void NLJBuild::execute(ExecutionContext& ctx, Record& record) const {
    /**
     * 1. Check if window is done
     * 2. Window bekommen durch OperatorHandler
     * 3. Pointer zu freiem Speicherbereich bekommen
     * 4. Record in den Speicherbereich reinschreiben
     */

    // Get the global state
    auto operatorHandlerMemRef = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    auto lastTupleWindowRef =
            Nautilus::FunctionCall("getLastTupleWindow", getLastTupleWindow, operatorHandlerMemRef, Value<Boolean>(isLeftSide));

    // Check if window is done
    if (record.read(timeStampField) > lastTupleWindowRef) {
        Nautilus::FunctionCall("triggerJoinSink",
                               triggerJoinSink,
                               operatorHandlerMemRef,
                               ctx.getPipelineContext(),
                               ctx.getWorkerContext(),
                               Value<Boolean>(isLeftSide));
    }

    // Get the pointer/memref to the new entry
    auto entryMemRef = Nautilus::FunctionCall();


    // Write Record at entryMemRef
    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
    for (auto& field : schema->fields) {
        auto const fieldName = field->getName();
        auto const fieldType = physicalDataTypeFactory.getPhysicalType(field->getDataType());

        entryMemRef.store(record.read(fieldName));
        entryMemRef = entryMemRef + fieldType->size();
    }
}

} // namespace NES::Runtime::Execution::Operators