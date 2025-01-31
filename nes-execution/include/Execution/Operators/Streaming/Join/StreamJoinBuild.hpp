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
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/Streaming/WindowOperatorBuild.hpp>
#include <Execution/Operators/Watermark/TimeFunction.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Util/Execution.hpp>

namespace NES::Runtime::Execution::Operators
{
/// This class is the first phase of the stream join. The actual implementation is not part of this class
/// It only takes care of the close() and terminate() functionality as these are universal
class StreamJoinBuild : public WindowOperatorBuild
{
public:
    StreamJoinBuild(
        uint64_t operatorHandlerIndex,
        QueryCompilation::JoinBuildSideType joinBuildSide,
        std::unique_ptr<TimeFunction> timeFunction,
        const std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider>& memoryProvider);

protected:
    const QueryCompilation::JoinBuildSideType joinBuildSide;
    const std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider> memoryProvider;
};

}
