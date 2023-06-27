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

#include <Experimental/Synopses/Sketches/CountMinOperatorHandler.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Nautilus/Interface/Hash/H3Hash.hpp>

#include <random>
namespace NES::ASP {

void CountMinOperatorHandler::start(Runtime::Execution::PipelineExecutionContextPtr,
                                    Runtime::StateManagerPtr, uint32_t) {
    NES_DEBUG("Started CountMinOperatorHandler!");
}

void CountMinOperatorHandler::stop(Runtime::QueryTerminationType,
                                   Runtime::Execution::PipelineExecutionContextPtr) {
    NES_DEBUG("Stopped CountMinOperatorHandler!");
}

void CountMinOperatorHandler::setup(uint64_t entrySize, uint64_t numberOfRows, uint64_t numberOfCols) {
    auto allocator = std::make_unique<Runtime::NesDefaultMemoryAllocator>();
    sketchArray = std::make_unique<Nautilus::Interface::Fixed2DArray>(*allocator, numberOfRows, numberOfCols, entrySize);

    // Creating the h3Seeds
    std::mt19937 gen(Nautilus::Interface::H3Hash::H3_SEED);
    std::uniform_int_distribution<uint64_t> distribution;
    for (auto row = 0UL; row < numberOfRows; ++row) {
        for (auto col = 0UL; col < numberOfKeyBits; ++col) {
            h3Seeds.emplace_back(distribution(gen));
        }
    }
}

void *CountMinOperatorHandler::getSketchRef() {
    return sketchArray.get();
}

void *CountMinOperatorHandler::getH3SeedsRef() {
    return h3Seeds.data();
}

CountMinOperatorHandler::CountMinOperatorHandler(const uint64_t numberOfKeyBits) : numberOfKeyBits(numberOfKeyBits) {}

} // namespace NES::ASP
