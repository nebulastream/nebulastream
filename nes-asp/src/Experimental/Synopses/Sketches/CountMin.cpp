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
#include <Experimental/Synopses/Sketches/CountMin.hpp>


namespace NES::ASP {

void CountMin::addToSynopsis(uint64_t handlerIndex, Runtime::Execution::ExecutionContext &ctx, Nautilus::Record record) {
    auto opHandlerMemRef = ctx.getGlobalOperatorHandler(handlerIndex);

    // hash
}

std::vector<Runtime::TupleBuffer> CountMin::getApproximate(uint64_t handlerIndex,
                                                           Runtime::Execution::ExecutionContext &ctx,
                                                           Runtime::BufferManagerPtr bufferManager) {

}

void CountMin::setup(uint64_t handlerIndex, Runtime::Execution::ExecutionContext &ctx) {

}

} // namespace NES::ASP