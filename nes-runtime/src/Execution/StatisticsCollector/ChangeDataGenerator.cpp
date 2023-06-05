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

#include <Execution/StatisticsCollector/ChangeDataGenerator.hpp>
#include <utility>

namespace NES::Runtime::Execution {


ChangeDataGenerator::ChangeDataGenerator(std::shared_ptr<Runtime::BufferManager> bm,
                                         Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayoutPtr,
                                         ChangeType changeType,
                                         int64_t noChangePeriod)
    : bm(std::move(bm)),
      memoryLayoutPtr(std::move(memoryLayoutPtr)),
      changeType(changeType),
      noChangePeriod(noChangePeriod),
      noChangeRemain(noChangePeriod){

    if (changeType == INCREMENTAL || changeType == REOCCURRING) {
        distributionF1Start = 5;
        distributionF2Start = 45;
    } else if (changeType == GRADUAL) {
        distributionF1Start = 1;
    }
}

std::vector<TupleBuffer> ChangeDataGenerator::generateBuffers(size_t numberOfBuffers) {

    // generate list of values 1 til 100
    uint64_t field1ValuesSize = 100;
    std::vector<int64_t> field1Values(field1ValuesSize);
    std::iota(std::begin(field1Values), std::end(field1Values), 1);

    std::vector<int64_t> field2Values(field1ValuesSize);
    std::iota(std::begin(field2Values), std::end(field2Values), 50);

    // generate tuple buffers
    std::vector<TupleBuffer> bufferVector;
    for (uint64_t i = 0; i < numberOfBuffers; ++i){
        auto buffer = bm->getBufferBlocking();
        bufferVector.push_back(buffer);
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayoutPtr, buffer);
        for (uint64_t j = 0; j < 10; j++) {
            std::shuffle(std::begin(field1Values), std::end(field1Values), rng);
            std::shuffle(std::begin(field2Values), std::end(field2Values), rng);
            for (uint64_t k = 0; k < field1ValuesSize; k++) {
                if (noChangeRemain > 0) {
                    noChangeRemain--;
                    dynamicBuffer[(j * field1ValuesSize) + k]["f1"].write(field1Values[k]);
                    dynamicBuffer[(j * field1ValuesSize) + k]["f2"].write(field2Values[k]);
                    dynamicBuffer.setNumberOfTuples((j * field1ValuesSize) + k + 1);
                } else {
                    field1Values = getNextValues();
                    if (changeType == REOCCURRING) field2Values = getNextF2Values();
                }
            }
        }
    }

    return bufferVector;
}

std::vector<int64_t> ChangeDataGenerator::getNextValues(){
    switch (changeType) {
        case ABRUPT: return getNextValuesAbrupt();
        case INCREMENTAL: return getNextValuesIncremental();
        case GRADUAL: return getNextValuesGradual();
        case REOCCURRING: return getNextValuesReoccurring();
    }
}

std::vector<int64_t> ChangeDataGenerator::getNextValuesAbrupt(){
    noChangeRemain = noChangePeriod;
    std::vector<int64_t> fieldValues(100);
    std::iota(std::begin(fieldValues), std::end(fieldValues), 50);
    std::shuffle(std::begin(fieldValues), std::end(fieldValues), rng);
    return fieldValues;
}

std::vector<int64_t> ChangeDataGenerator::getNextValuesIncremental(){
    noChangeRemain = noChangePeriod;
    std::vector<int64_t> fieldValues(100);
    std::iota(std::begin(fieldValues), std::end(fieldValues), distributionF1Start);
    std::shuffle(std::begin(fieldValues), std::end(fieldValues), rng);
    distributionF1Start += incrementSteps;
    return fieldValues;
}

std::vector<int64_t> ChangeDataGenerator::getNextValuesGradual(){
    if (distributionF1Start == 1 || gradualDeclinePeriod < 0){
        distributionF1Start = 50;
        noChangeRemain = gradualChangePeriod;
        gradualChangePeriod += 20000;
    } else {
        distributionF1Start = 1;
        noChangeRemain = gradualDeclinePeriod;
        gradualDeclinePeriod -= 20000;
    }
    std::vector<int64_t> fieldValues(100);
    std::iota(std::begin(fieldValues), std::end(fieldValues), distributionF1Start);
    std::shuffle(std::begin(fieldValues), std::end(fieldValues), rng);
    return fieldValues;
}

std::vector<int64_t> ChangeDataGenerator::getNextValuesReoccurring(){
    std::vector<int64_t> fieldValues(100);
    if(!reoccur) {
        noChangeRemain = noChangePeriod;
        std::iota(std::begin(fieldValues), std::end(fieldValues), distributionF1Start);
        std::shuffle(std::begin(fieldValues), std::end(fieldValues), rng);
        distributionF1Start += incrementSteps;
    } else {
        noChangeRemain = noChangePeriod;
        std::iota(std::begin(fieldValues), std::end(fieldValues), distributionF1Start);
        std::shuffle(std::begin(fieldValues), std::end(fieldValues), rng);
        distributionF1Start -= incrementSteps;
        if (distributionF1Start == 0) distributionF1Start = 1;
    }
    return fieldValues;
}
std::vector<int64_t> ChangeDataGenerator::getNextF2Values() {
    std::vector<int64_t> fieldValues(100);
    if(!reoccur) {
        noChangeRemain = noChangePeriod;
        std::iota(std::begin(fieldValues), std::end(fieldValues), distributionF2Start);
        std::shuffle(std::begin(fieldValues), std::end(fieldValues), rng);
        distributionF2Start -= incrementSteps;
        if (distributionF2Start == 0) {
            distributionF2Start = 1;
            reoccur = true;
        }
    } else {
        noChangeRemain = noChangePeriod;
        std::iota(std::begin(fieldValues), std::end(fieldValues), distributionF2Start);
        std::shuffle(std::begin(fieldValues), std::end(fieldValues), rng);
        if (distributionF2Start == 1){
            distributionF2Start = 5;
        } else {
            distributionF2Start += incrementSteps;
        }
    }
    return fieldValues;
}

}// namespace NES::Runtime::Execution