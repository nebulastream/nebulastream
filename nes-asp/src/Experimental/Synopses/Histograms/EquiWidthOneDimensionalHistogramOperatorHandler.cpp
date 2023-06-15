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

#include <Experimental/Synopses/Histograms/EquiWidthOneDimensionalHistogramOperatorHandler.hpp>
#include <Nautilus/Interface/Fixed2DArray/Fixed2DArray.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>

namespace NES::ASP {

void * EquiWidthOneDimensionalHistogramOperatorHandler::getBinsRef() { return bins.get(); }

void EquiWidthOneDimensionalHistogramOperatorHandler::setup(uint64_t entrySize, uint64_t numberOfBins) {
    auto allocator = std::make_unique<Runtime::NesDefaultMemoryAllocator>();
    auto numRows = 1; // For now, we just use a single row for all bins.
    bins = std::make_unique<Nautilus::Interface::Fixed2DArray>(*allocator, numRows, numberOfBins, entrySize);
}

void EquiWidthOneDimensionalHistogramOperatorHandler::start(Runtime::Execution::PipelineExecutionContextPtr,
                                           Runtime::StateManagerPtr, uint32_t) {
    NES_DEBUG2("Started EquiWidth1DHistOperatorHandler!");
}

void EquiWidthOneDimensionalHistogramOperatorHandler::stop(Runtime::QueryTerminationType,
                                          Runtime::Execution::PipelineExecutionContextPtr) {
    NES_DEBUG2("Started EquiWidth1DHistOperatorHandler!");
}

} // namespace NES::ASP