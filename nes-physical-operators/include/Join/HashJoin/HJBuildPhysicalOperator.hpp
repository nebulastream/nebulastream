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
#include <Identifiers/Identifiers.hpp>
#include <Join/HashJoin/HJOperatorHandler.hpp>
#include <Join/StreamJoinBuildPhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Time/Timestamp.hpp>
#include <Watermark/TimeFunction.hpp>
#include <ExecutionContext.hpp>
#include <HashMapOptions.hpp>

namespace NES
{
class HJBuildPhysicalOperator;
Interface::HashMap* getHashJoinHashMapProxy(
    const HJOperatorHandler* operatorHandler,
    Timestamp timestamp,
    WorkerThreadId workerThreadId,
    JoinBuildSideType buildSide,
    const HJBuildPhysicalOperator* buildOperator);

/// This class is the first phase of the join. For both streams (left and right), the tuples are stored in a hash map of a
/// corresponding slice one after the other. Afterward, the second phase (HJProbe) will start joining the tuples by comparing the join keys
/// via a hash function.
class HJBuildPhysicalOperator final : public StreamJoinBuildPhysicalOperator
{
public:
    friend Interface::HashMap* getHashJoinHashMapProxy(
        const HJOperatorHandler* operatorHandler,
        Timestamp timestamp,
        WorkerThreadId workerThreadId,
        JoinBuildSideType buildSide,
        const HJBuildPhysicalOperator* buildOperator);
    HJBuildPhysicalOperator(
        OperatorHandlerId operatorHandlerId,
        JoinBuildSideType joinBuildSide,
        std::unique_ptr<TimeFunction> timeFunction,
        const std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider>& memoryProvider,
        HashMapOptions hashMapOptions);
    void setup(ExecutionContext& executionCtx) const override;
    void execute(ExecutionContext& ctx, Record& record) const override;

private:
    HashMapOptions hashMapOptions;
};

}
