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
#include <Execution/StatisticsCollector/ChangeDetectors/SeqDrift2/Repository.hpp>

namespace NES::Runtime::Execution {

Repository::Repository(int64_t blockSize):
    listOfBlocks(),
    blockSize(blockSize),
    indexOfLastBlock(-1),
    instanceCount(0),
    total(0)
{}

void Repository::add(double value) {
    if((instanceCount % blockSize) == 0){
        Block block = Block(blockSize);
        listOfBlocks.push_back(block);
        indexOfLastBlock++;
    }
    listOfBlocks.back().add(value);
    instanceCount++;
    total = total + value;
}

double Repository::get(int64_t index) {
    return listOfBlocks[index/blockSize].data[(index % blockSize)];
}

void Repository::addAt(int64_t index, double value) {
    listOfBlocks[index/blockSize].addAtIndex(index % blockSize, value);
}

int64_t Repository::getSize() const {
    return instanceCount;
}

void Repository::removeAll() {
    listOfBlocks.clear();
    indexOfLastBlock = -1;
    instanceCount = 0;
    total = 0;
}

} // namespace NES::Runtime::Execution