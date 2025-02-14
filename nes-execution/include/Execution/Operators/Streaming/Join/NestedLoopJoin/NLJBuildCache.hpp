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

#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJSlice.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinBuild.hpp>
#include <Execution/Operators/Watermark/TimeFunction.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <QueryCompiler/Configurations/QueryCompilerConfiguration.hpp>


namespace NES::Runtime::Execution::Operators
{

/// This class is the first phase of the join. For both streams (left and right), the tuples are stored in the
/// corresponding slice one after the other. Afterward, the second phase (NLJProbe) will start joining the tuples
/// via two nested loops. This class uses a slice cache to cache previously used slices.
class NLJBuildCache final : public StreamJoinBuild
{
public:
    NLJBuildCache(
        uint64_t operatorHandlerIndex,
        QueryCompilation::JoinBuildSideType joinBuildSide,
        std::unique_ptr<TimeFunction> timeFunction,
        const std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider>& memoryProvider,
        const QueryCompilation::Configurations::SliceCacheOptions& sliceCacheOptions);

    void setup(ExecutionContext& executionCtx) const override;
    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void execute(ExecutionContext& executionCtx, Record& record) const override;
    void terminate(ExecutionContext &executionCtx) const override;

private:
    /// This might not be the best place to store it, but it is an easy way to use them in this PoC branch
    QueryCompilation::Configurations::SliceCacheOptions sliceCacheOptions;
};

}