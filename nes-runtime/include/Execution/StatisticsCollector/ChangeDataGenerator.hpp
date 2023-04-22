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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_CHANGEDATAGENERATOR_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_CHANGEDATAGENERATOR_HPP_

#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/MemoryLayout/ColumnLayout.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <random>

namespace NES::Runtime::Execution {

enum ChangeType {
    ABRUPT, INCREMENTAL, GRADUAL , REOCCURRING
};

class ChangeDataGenerator {

  public:
    ChangeDataGenerator(std::shared_ptr<Runtime::BufferManager> bm,
                        Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayoutPtr,
                        ChangeType changeType,
                        int64_t noChangePeriod);

    std::vector<TupleBuffer> generateBuffers(size_t numberOfBuffers);

  private:
    std::vector<int64_t> getNextValues();
    std::vector<int64_t> getNextValuesAbrupt();
    std::vector<int64_t> getNextValuesIncremental();
    std::vector<int64_t> getNextValuesGradual();
    std::vector<int64_t> getNextValuesReoccurring();

    std::shared_ptr<Runtime::BufferManager> bm;
    Runtime::MemoryLayouts::MemoryLayoutPtr memoryLayoutPtr;
    ChangeType changeType;
    int64_t noChangePeriod;
    int64_t noChangeRemain;
    int64_t distributionStart;
    int64_t incrementSteps = 5;
    int64_t gradualChangePeriod = 500;
    int64_t gradualDeclinePeriod = 5000;
    std::default_random_engine rng = std::default_random_engine {};

};
}// namespace NES::Runtime::Execution
#endif// NES_RUNTIME_INCLUDE_EXECUTION_CHANGEDATAGENERATOR_HPP_