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

#include <Experimental/Synopses/Histograms/EquiWidth1DHist.hpp>
namespace NES::ASP {

void EquiWidth1DHist::addToSynopsis(uint64_t handlerIndex, Runtime::Execution::ExecutionContext &ctx,
                                    Nautilus::Record record) {

}

std::vector<Runtime::TupleBuffer>
EquiWidth1DHist::getApproximate(uint64_t handlerIndex, Runtime::Execution::ExecutionContext &ctx,
                                Runtime::BufferManagerPtr bufferManager) {

}

void EquiWidth1DHist::setup(uint64_t handlerIndex, Runtime::Execution::ExecutionContext &ctx) {

}
} // namespace NES::ASP