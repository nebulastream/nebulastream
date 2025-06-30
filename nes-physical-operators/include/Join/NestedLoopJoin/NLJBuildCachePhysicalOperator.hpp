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
#include <Configurations/Worker/SliceCacheConfiguration.hpp>
#include <Join/NestedLoopJoin/NLJBuildPhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Watermark/TimeFunction.hpp>

namespace NES
{


class NLJBuildCachePhysicalOperator final : public NLJBuildPhysicalOperator
{
public:
    NLJBuildCachePhysicalOperator(
        OperatorHandlerId operatorHandlerId,
        JoinBuildSideType joinBuildSide,
        std::unique_ptr<TimeFunction> timeFunction,
        std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider> memoryProvider,
        NES::Configurations::SliceCacheOptions sliceCacheOptions);
    ~NLJBuildCachePhysicalOperator() override = default;

    NLJBuildCachePhysicalOperator(const NLJBuildCachePhysicalOperator& other) = default;

    void setup(ExecutionContext& executionCtx, const nautilus::engine::NautilusEngine& engine) const override;
    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void execute(ExecutionContext& executionCtx, Record& record) const override;

private:
    /// This might not be the best place to store it, but it is an easy way to use them in this PoC branch
    NES::Configurations::SliceCacheOptions sliceCacheOptions;
};
}
