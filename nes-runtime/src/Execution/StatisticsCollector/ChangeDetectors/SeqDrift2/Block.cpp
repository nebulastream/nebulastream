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
#include <Execution/StatisticsCollector/ChangeDetectors/SeqDrift2/Block.hpp>
#include <iostream>

namespace NES::Runtime::Execution {

Block::Block(int64_t length):
   data(length, -1.0),
   total(0),
   indexOfLastValue(0)
{}

void Block::add(double value) {
    if (indexOfLastValue < (int64_t) data.size()){
        data[indexOfLastValue] = value;
        total = total + value;
        indexOfLastValue++;
    } else {
        std::cout << "Error adding to block";
    }
}

void Block::addAtIndex(int64_t index, double newValue) {
    total = total - data[index] + newValue;
    data[index] = newValue;
}

} // namespace NES::Runtime::Execution