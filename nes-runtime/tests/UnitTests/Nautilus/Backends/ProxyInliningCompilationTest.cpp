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

#include "Nautilus/Util/CompilationOptions.hpp"
#include <Execution/TupleBufferProxyFunctions.hpp>
#include <Nautilus/Backends/BCInterpreter/ByteCode.hpp>
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Tracing/Trace/ExecutionTrace.hpp>
#include <Nautilus/Tracing/TraceContext.hpp>
#include <NesBaseTest.hpp>
#include <Runtime/BufferManager.hpp>
#include <TestUtils/AbstractCompilationBackendTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <fstream>
#include <memory>

namespace NES::Nautilus {

class ProxyFunctionInliningCompilationTest : public Testing::NESBaseTest, public AbstractCompilationBackendTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ProxyFunctionInliningCompilationTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup ProxyFunctionInliningCompilationTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down TraceTest test class."); }
};

Value<UInt64> getNumberOfTuples(Value<MemRef> tupleBufferRef) {
    return FunctionCall<>("NES__Runtime__TupleBuffer__getNumberOfTuples",
                          NES::Runtime::ProxyFunctions::NES__Runtime__TupleBuffer__getNumberOfTuples,
                          tupleBufferRef);
}

void executeFunctionWithOptions(CompilationOptions& options) {
    // Create TupleBuffer and generate execution trace.
    auto bufferManager = std::make_unique<Runtime::BufferManager>();
    auto tupleBuffer = bufferManager->getBufferNoBlocking();
    Value<MemRef> tupleBufferPointer = Value<MemRef>((int8_t*) std::addressof(tupleBuffer));
    tupleBufferPointer.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 0, IR::Types::StampFactory::createAddressStamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([&]() {
        return getNumberOfTuples(tupleBufferPointer);
    });

    // Create file name to write LLVM IR file with inlined proxy functions to. Set proxy inlining to true in options.
    const std::time_t t_c = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::ostringstream datetime;
    datetime << std::put_time(std::localtime(&t_c), "%F-%T");
    const auto outputFileName = std::filesystem::temp_directory_path().string() 
                                + "/" + datetime.str() + "-ProxyInliningCompilationTest.ll";
    options.setProxyInlining(true, outputFileName);
    options.setDumpToFile(true);

    // Execute function that contains 'getNumberOfTuples' as external function call.
    AbstractCompilationBackendTest abstractCompilationBackendTest{};
    auto result = abstractCompilationBackendTest.prepare(executionTrace, options);
    auto function = result->getInvocableMember<uint64_t, uint8_t*>("execute");
    NES_DEBUG("Function result: " << function((uint8_t*) std::addressof(tupleBuffer)));
}

TEST_P(ProxyFunctionInliningCompilationTest, getNumberOfTuplesInliningTest) {
    CompilationOptions options;
    executeFunctionWithOptions(options);
    std::ifstream generatedProxyIR(options.getProxyInliningOutputPath());
    NES_ASSERT2_FMT(generatedProxyIR.peek() != std::ifstream::traits_type::eof(), "No proxy file was generated.");
    bool foundExecute = false;
    std::string line;
    while (std::getline(generatedProxyIR, line)) {
        if (!foundExecute) {
            if (line.find("@execute") != std::string::npos) {
                foundExecute = true;
            }
        } else {
            // The generated execute function should not contain any tail calls.
            NES_ASSERT2_FMT(line.find("@tail call") == std::string::npos, "execute contained a tail call even though \
            all tail calls should have been removed via proxy function inlining.");
            // Check if we reached the end of the execute function.
            if (line == "}") {
                break;
            }
        }
    }
    std::remove(options.getProxyInliningOutputPath().c_str());
    generatedProxyIR.close();
}

TEST_P(ProxyFunctionInliningCompilationTest, getNumberOfTuplesInliningWithoutOptimizationTest) {
    CompilationOptions options;
    options.setOptimizationLevel(0);
    executeFunctionWithOptions(options);
    std::ifstream generatedProxyIR(options.getProxyInliningOutputPath());
    NES_ASSERT2_FMT(generatedProxyIR.peek() != std::ifstream::traits_type::eof(), "No proxy file was generated.");
    bool foundExecute = false;
    bool callFound = false;
    // When compiling with O0, the generated code should contain a function call to getNumberOfTuples.
    std::string line;
    while (std::getline(generatedProxyIR, line)) {
        if (!foundExecute) {
            if (line.find("@execute") != std::string::npos) {
                foundExecute = true;
            }
        } else {
            callFound = callFound || (line.find("call") == std::string::npos);
            // Check if we reached the end of the execute function.
            if (line == "}") {
                break;
            }
        }
    }
    NES_ASSERT2_FMT(callFound, "The execute function should contain a call to getNumberOfTuples when compiling with \
                                optimization level O0");
    std::remove(options.getProxyInliningOutputPath().c_str());
    generatedProxyIR.close();
}

// Tests all registered compilation backends.
auto pluginNames = Backends::CompilationBackendRegistry::getPluginNames();
INSTANTIATE_TEST_CASE_P(testFunctionCalls,
                        ProxyFunctionInliningCompilationTest,
                        // ::testing::ValuesIn(pluginNames.begin(), pluginNames.end()),
                        ::testing::Values("MLIR"),
                        [](const testing::TestParamInfo<ProxyFunctionInliningCompilationTest::ParamType>& info) {
                            return info.param;
                        });

}// namespace NES::Nautilus