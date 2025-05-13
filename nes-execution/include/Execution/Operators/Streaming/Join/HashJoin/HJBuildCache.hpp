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
#include <QueryCompiler/Configurations/Enums/WindowManagement.hpp>

namespace NES::Runtime::Execution::Operators
{

class HJBuildCache;
int8_t* createNewHJSliceProxy(
    SliceCacheEntry* sliceCacheEntry,
    OperatorHandler* ptrOpHandler,
    const Timestamp timestamp,
    const WorkerThreadId workerThreadId,
    const QueryCompilation::JoinBuildSideType joinBuildSide,
    const HJBuildCache* buildOperator);

class HJBuildCache final : public HashMapOptions, public StreamJoinBuild
{
public:
    friend int8_t* createNewHJSliceProxy(
        SliceCacheEntry* sliceCacheEntry,
        OperatorHandler* ptrOpHandler,
        const Timestamp timestamp,
        const WorkerThreadId workerThreadId,
        const QueryCompilation::JoinBuildSideType joinBuildSide,
        const HJBuildCache* buildOperator);
    HJBuildCache(
        uint64_t operatorHandlerIndex,
        QueryCompilation::JoinBuildSideType joinBuildSide,
        std::unique_ptr<TimeFunction> timeFunction,
        const std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider>& memoryProvider,
        HashMapOptions hashMapOptions,
        QueryCompilation::Configurations::SliceCacheOptions sliceCacheOptions);


    void setup(ExecutionContext& executionCtx) const override;
    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void execute(ExecutionContext& executionCtx, Record& record) const override;


private:
    /// This might not be the best place to store it, but it is an easy way to use them in this PoC branch
    QueryCompilation::Configurations::SliceCacheOptions sliceCacheOptions;
};

}
