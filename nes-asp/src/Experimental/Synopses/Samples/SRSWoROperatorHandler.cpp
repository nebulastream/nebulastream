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


#include <Experimental/Synopses/Samples/SRSWoROperatorHandler.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>

namespace NES::ASP {

void* SRSWoROperatorHandler::getPagedVectorRef() { return pagedVector.get(); }

void SRSWoROperatorHandler::setup(uint64_t entrySize) {
    auto allocator = std::make_unique<Runtime::NesDefaultMemoryAllocator>();
    pagedVector = std::make_unique<Nautilus::Interface::PagedVector>(std::move(allocator), entrySize);
}
void SRSWoROperatorHandler::start(Runtime::Execution::PipelineExecutionContextPtr,
                                  Runtime::StateManagerPtr,
                                  uint32_t) {
    NES_DEBUG2("Started SRSWoROperatorHandler!");
}
void SRSWoROperatorHandler::stop(Runtime::QueryTerminationType,
                                 Runtime::Execution::PipelineExecutionContextPtr) {
    NES_DEBUG2("Stopped SRSWoROperatorHandler!");
}

} // namespace NES::ASP