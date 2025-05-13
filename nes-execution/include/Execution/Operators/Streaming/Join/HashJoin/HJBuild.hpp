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
#include <Execution/Operators/Streaming/HashMapOptions.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/HJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinBuild.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedEntryMemoryProvider.hpp>

namespace NES::Runtime::Execution::Operators
{


class HJBuild;
Interface::HashMap* getHashJoinHashMapProxy(
    const HJOperatorHandler* operatorHandler,
    const Timestamp timestamp,
    const WorkerThreadId workerThreadId,
    const QueryCompilation::JoinBuildSideType buildSide,
    const HJBuild* buildOperator);

/// This class is the first phase of the join. For both streams (left and right), the tuples are stored in a hash map of a
/// corresponding slice one after the other. Afterward, the second phase (HJProbe) will start joining the tuples by comparing the join keys
/// via a hash function.
class HJBuild final : public StreamJoinBuild, public HashMapOptions
{
public:
    friend Interface::HashMap* getHashJoinHashMapProxy(
        const HJOperatorHandler* operatorHandler,
        const Timestamp timestamp,
        const WorkerThreadId workerThreadId,
        const QueryCompilation::JoinBuildSideType buildSide,
        const HJBuild* buildOperator);
    HJBuild(
        uint64_t operatorHandlerIndex,
        QueryCompilation::JoinBuildSideType joinBuildSide,
        std::unique_ptr<TimeFunction> timeFunction,
        const std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider>& memoryProvider,
        HashMapOptions hashMapOptions);
    void setup(ExecutionContext& executionCtx) const override;
    void execute(ExecutionContext& ctx, Record& record) const override;
};

}
