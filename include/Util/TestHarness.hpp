/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_TESTHARNESS_HPP
#define NES_TESTHARNESS_HPP
#include <API/Schema.hpp>
#include <Operators/OperatorNode.hpp>
/**
 * @brief This test harness wrap query deployment test in our test framework.
 */
namespace NES {
    class TestHarness {
      public:
        /*
         * @brief The constructor of TestHarness
         * @param numWorkers number of worker (each for one physical source) to be used in the test
         * @param operatorToTest operator to test
         */
        TestHarness(uint64_t numWorkers, std::string operatorToTest);

        /*
         * @brief predefined struct of the records
         */
        struct Record {
            uint32_t key;
            uint32_t value;
            uint64_t timestamp;
        };

        /*
         * @brief push a single element/tuple to specific source
         * @param element element of Record to push
         * @param sourceIdx index of the source to which the element is pushed
         */
        void pushElement(Record element, uint64_t sourceIdx);

        /*
         * @brief execute the test based on the given operator, pushed elements, and number of workers,
         * then return the result of the query execution
         * @return output string
         */
        std::string getOutput();

      private:
        NesCoordinatorPtr crd;

        SchemaPtr schema;
        uint64_t numWorkers;
        std::string operatorToTest;

        std::vector<AbstractPhysicalStreamConfigPtr> physicalStreamConfigurationPtrs;
        std::vector<NesWorkerPtr> workerPtrs;
        std::vector<std::vector<Record>> records;
        std::vector<uint64_t> counters;

    };

    typedef std::shared_ptr<TestHarness> TestHarnessPtr;
} // namespace NES


#endif//NES_TESTHARNESS_HPP
