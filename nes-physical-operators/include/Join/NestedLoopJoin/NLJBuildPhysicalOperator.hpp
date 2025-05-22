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
#include <Join/StreamJoinBuildPhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Watermark/TimeFunction.hpp>

namespace NES
{

/// This class is the first phase of the join. For both streams (left and right), the tuples are stored in the
/// corresponding slice one after the other. Afterward, the second phase (NLJProbe) will start joining the tuples
/// via two nested loops.
class NLJBuildPhysicalOperator final : public StreamJoinBuildPhysicalOperator
{
public:
    NLJBuildPhysicalOperator(
        OperatorHandlerId operatorHandlerId,
        JoinBuildSideType joinBuildSide,
        std::unique_ptr<TimeFunction> timeFunction,
        std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider> memoryProvider);

    NLJBuildPhysicalOperator(const NLJBuildPhysicalOperator& other) = default;

    void execute(ExecutionContext& executionCtx, Record& record) const override;
};
}
