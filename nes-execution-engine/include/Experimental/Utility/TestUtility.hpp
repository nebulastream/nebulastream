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

#ifndef NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_UTILITY_TESTUTILITY_HPP_
#define NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_UTILITY_TESTUTILITY_HPP_

#include <Nautilus/Backends/MLIR/LLVMIROptimizer.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>

namespace NES::ExecutionEngine::Experimental {
class TestUtility {
    public:
    struct YsbScaleConfig {
        const int BUFFER_SIZE;
        const int NUMBER_OF_BUFFERS;
    };
    struct TestParameterConfig {
        const NES::Nautilus::Backends::MLIR::LLVMIROptimizer::OptimizationLevel OPT_LEVEL;
        const bool PERFORM_INLINING;
        const int NUM_ITERATIONS;
        const int NUM_SNAPSHOTS; // 7 -> 8
        const std::string RESULTS_FILE_NAME;
        const std::vector<std::string> snapshotNames;
        const YsbScaleConfig ysbConfig;
        const bool IS_PERFORMANCE_BENCHMARK;
    };
    TestUtility() = default;
    ~TestUtility() = default;

    std::pair<std::shared_ptr<NES::Runtime::MemoryLayouts::RowLayout>, NES::Runtime::MemoryLayouts::DynamicTupleBuffer> 
        loadLineItemTable(std::shared_ptr<Runtime::BufferManager> bm);
    void produceResults(std::vector<std::vector<double>> runningSnapshotVectors, 
                        std::vector<std::string> snapshotNames, const std::string &resultsFileName, 
                        const bool performanceBenchmark = false, bool writeRawData = true, const int NUM_ITERATIONS=101);
    std::vector<std::string> loadStringsFromLineitemTable();
    const std::unique_ptr<TestParameterConfig> getTestParamaterConfig(const std::string &resultsFileName);
};
}
#endif //NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_UTILITY_TESTUTILITY_HPP_