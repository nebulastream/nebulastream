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
        distributionStart = 5;
    } else if (changeType == GRADUAL) {
        distributionStart = 1;
    }
}

std::vector<TupleBuffer> ChangeDataGenerator::generateBuffers(size_t numberOfBuffers) {

    std::vector<TupleBuffer> bufferVector;

    // generate list of values 0 til 100
    std::vector<int64_t> fieldValues(100);
    std::iota(std::begin(fieldValues), std::end(fieldValues), 1);

    for (size_t i = 0; i < numberOfBuffers; ++i) {
        auto buffer = bm->getBufferBlocking();
        bufferVector.push_back(buffer);

        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayoutPtr, buffer);

        for (int k = 0; k < 5; k++) {
            std::shuffle(std::begin(fieldValues), std::end(fieldValues), rng);
            for (uint64_t j = 0; j < 100; j++) {
                if (noChangeRemain > 0) {
                    noChangeRemain--;
                    dynamicBuffer[j]["f1"].write(fieldValues[j]);
                    dynamicBuffer[j]["f2"].write((int64_t) 1);
                    dynamicBuffer.setNumberOfTuples(j + 1);
                } else {
                    fieldValues = getNextValues();
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
    std::iota(std::begin(fieldValues), std::end(fieldValues), distributionStart);
    std::shuffle(std::begin(fieldValues), std::end(fieldValues), rng);
    distributionStart += incrementSteps;
    return fieldValues;
}

std::vector<int64_t> ChangeDataGenerator::getNextValuesReoccurring(){
    std::vector<int64_t> fieldValues(100);
    if(!reoccur) {
        noChangeRemain = noChangePeriod;
        std::iota(std::begin(fieldValues), std::end(fieldValues), distributionStart);
        std::shuffle(std::begin(fieldValues), std::end(fieldValues), rng);
        distributionStart += incrementSteps;
        if (distributionStart == 50) reoccur = true;
    } else {
        noChangeRemain = noChangePeriod;
        std::iota(std::begin(fieldValues), std::end(fieldValues), distributionStart);
        std::shuffle(std::begin(fieldValues), std::end(fieldValues), rng);
        distributionStart -= incrementSteps;
        if (distributionStart == 0) distributionStart = 1;
    }
    return fieldValues;
}

std::vector<int64_t> ChangeDataGenerator::getNextValuesGradual(){
    if (distributionStart == 1 || gradualDeclinePeriod < 0){
        distributionStart = 50;
        noChangeRemain = gradualChangePeriod;
        gradualChangePeriod += 500;
    } else {
        distributionStart = 1;
        noChangeRemain = gradualDeclinePeriod;
        gradualDeclinePeriod -= 500;
    }
    std::vector<int64_t> fieldValues(100);
    std::iota(std::begin(fieldValues), std::end(fieldValues), distributionStart);
    std::shuffle(std::begin(fieldValues), std::end(fieldValues), rng);
    return fieldValues;
}

}// namespace NES::Runtime::Execution