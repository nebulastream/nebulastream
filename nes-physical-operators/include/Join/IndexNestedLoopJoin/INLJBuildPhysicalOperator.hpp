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
#include <Interface/BTree/BTreeRef.hpp>
#include <Interface/Record.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <SliceStore/SliceStoreRef.hpp>
#include <Watermark/TimeFunction.hpp>
#include <WindowBuildPhysicalOperator.hpp>

namespace NES
{

class INLJBuildPhysicalOperator final : public WindowBuildPhysicalOperator
{
public:
    INLJBuildPhysicalOperator(
        OperatorHandlerId operatorHandlerId,
        JoinBuildSideType joinBuildSide,
        std::unique_ptr<TimeFunction> timeFunction,
        std::shared_ptr<BTreeTupleLayout> tupleLayout,
        Record::RecordFieldIdentifier keyField,
        std::unique_ptr<SliceStoreRef> sliceStoreRef);

    void setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const override;
    void execute(ExecutionContext& executionCtx, Record& record) const override;

private:
    JoinBuildSideType joinBuildSide;
    std::shared_ptr<BTreeTupleLayout> tupleLayout;
    Record::RecordFieldIdentifier keyField;
    mutable std::vector<std::shared_ptr<BTreeComparator>> comparators;
    mutable BTreeComparator* activeComparator = nullptr;
};

}
