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

#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Sinks/Formats/Synopses/CountMin.hpp>
#include <cmath>

namespace NES::Experimental::Statistics {
CountMin::CountMin(uint64_t** data,
                   double error,
                   double probability,
                   const std::string& logicalSourceName,
                   const std::string& physicalSourceName,
                   const std::string& fieldName,
                   const NES::TopologyNodeId nodeId,
                   const uint64_t statisticCollectorStorageKey,
                   const uint64_t observedTuples,
                   const time_t startTime,
                   const time_t endTime)
    : Synopsis(logicalSourceName,
               physicalSourceName,
               fieldName,
               nodeId,
               statisticCollectorStorageKey,
               observedTuples,
               startTime,
               endTime),
      data(data), depth(calcDepth(probability)), width(calcWidth(error)), error(error), probability(probability) {}

uint64_t CountMin::calcDepth(double probability) { return std::ceil(M_E / probability); }

uint64_t CountMin::calcWidth(double error) { return std::ceil(std::log(1 / error)); }

//CountMin& CountMin::createCountMinFromTuple(uint64_t depth, uint64_t width, uint8_t* startAddress, uint64_t sizeOfTextField) {
//    if(depth * width * sizeof(uint64_t) == sizeOfTextField) {
//        uint64_t** array = new uint64_t*[depth];
//
//        for (size_t i = 0; i < depth; ++i) {
//            array[i] = new uint64_t[width];
//        }
//
//        auto address = startAddress;
//        for (uint64_t column = 0; column < depth; column++) {
//            for (uint64_t row = 0; row < width; width++) {
//                std::string deserialized(address, address + sizeof(uint64_t));
////                auto value = std::strtoull(deserialized);
//            }
//        }
////        auto cm = new CountMin(depth, width, startAddress);
//    }
//}

void CountMin::writeToBuffer(std::vector<Synopsis>& synopses, Runtime::MemoryLayouts::DynamicTupleBuffer& buffer) {
    auto x = synopses.size();
    auto y = buffer.getBuffer();
    auto z = data[0][0];
    auto depth = this->getDepth();
    auto width = this->getWidth();
    auto error = this->getError();
    auto probability = this->getProbability();
}

std::vector<Synopsis> CountMin::readFromBuffer(Runtime::MemoryLayouts::DynamicTupleBuffer& dynBuffer) {

    std::vector<Synopsis> synopses;

    auto depth = dynBuffer[0]["depth"].read<uint64_t>();
    auto width = dynBuffer[0]["width"].read<uint64_t>();
    for (uint64_t rowIdx = 0; rowIdx < dynBuffer.getNumberOfTuples(); rowIdx++) {
        auto childBufferIdx = dynBuffer[rowIdx]["data"].read<uint32_t>();
        auto buf = dynBuffer.getBuffer();
        auto sizeOfTextField = *(buf.getBuffer<uint32_t>()) - sizeof(uint32_t);
        auto startAddress = buf.loadChildBuffer(childBufferIdx).getBuffer() + sizeof(uint32_t);

//        auto countMin = CountMin::createCountMinFromTuple(depth, width, startAddress, sizeOfTextField);
//        synopses.push_back(countMin);
    }

    return synopses;
}

uint64_t CountMin::getDepth() const { return depth; }

uint64_t CountMin::getWidth() const { return width; }

double CountMin::getError() const { return error; }

double CountMin::getProbability() const { return probability; }

}// namespace NES::Experimental::Statistics