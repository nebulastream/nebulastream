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

#include <Execution/StatisticsCollector/ChangeDetectors/SeqDrift2/Reservoir.hpp>
#include <random>
#include <iostream>

namespace NES::Runtime::Execution {

Reservoir::Reservoir(int sampleSize, int blockSize) : size(0),
      total(0),
      //blockSize(blockSize),
      instanceCount(0),
      sampleSize(sampleSize),
      repository(blockSize){}

void Reservoir::addElement(double value) { // reservoir sampling from the repository
    if(size < sampleSize){
        repository.add(value);
        total = total + value;
        size++;
    } else {
        srand(time(NULL));
        auto position = rand() % (instanceCount + 1);
        std::random_device rd;
        std::mt19937 rng(rd());
        auto dist = std::uniform_int_distribution<int64_t>(0, (instanceCount + 1));
        auto pos = dist(rng);
        std::cout << "position: " << position << " pos: " << pos << std::endl;
        if(position < sampleSize) {
            total = total - repository.get(position);
            repository.addAt(value, position);
            total = total + value;
        }
    }
    instanceCount++;
}

double Reservoir::get(int index) {
    return repository.get(index);
}

double Reservoir::getSampleMean() const {
    return total / size;
}

int Reservoir::getSize() const {
    return size;
}

double Reservoir::getTotal() const {
    return total;
}

void Reservoir::setSampleSize(int sampleSize) {
    Reservoir::sampleSize = sampleSize;
}

void Reservoir::clear() {
    repository.removeAll();
    total = 0;
    size = 0;
}

void Reservoir::copy(Reservoir& source) {
    for (int i = 0; i < source.getSize(); i++) {
        addElement(source.get(i));
    }
    source.clear();
}

} // namespace NES::Runtime::Execution