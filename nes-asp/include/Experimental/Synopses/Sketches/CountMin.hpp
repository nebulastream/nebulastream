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

#ifndef NES_COUNTMIN_HPP
#define NES_COUNTMIN_HPP

#include <Experimental/Synopses/AbstractSynopsis.hpp>


namespace NES::ASP {
class CountMin : public AbstractSynopsis {

public:
    void
    addToSynopsis(uint64_t handlerIndex, Runtime::Execution::ExecutionContext &ctx, Nautilus::Record record) override;

    std::vector<Runtime::TupleBuffer> getApproximate(uint64_t handlerIndex, Runtime::Execution::ExecutionContext &ctx,
                                                     Runtime::BufferManagerPtr bufferManager) override;

    void setup(uint64_t handlerIndex, Runtime::Execution::ExecutionContext &ctx) override;

    ~CountMin() override = default;

};
} // namespace NES::ASP

#endif //NES_COUNTMIN_HPP
