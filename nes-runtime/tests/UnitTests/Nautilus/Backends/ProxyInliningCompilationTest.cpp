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

#include <Experimental/Interpreter/ProxyFunctions.hpp>
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
#include <memory>
#include <fstream>

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
TEST_P(ProxyFunctionInliningCompilationTest, addIntFunctionTest) {
    auto bufferManager = std::make_unique<Runtime::BufferManager>();
    auto tupleBuffer = bufferManager->getBufferNoBlocking();
    tupleBuffer->setNumberOfTuples(3);
    Value<MemRef> tupleBufferPointer = Value<MemRef>((int8_t*) std::addressof(tupleBuffer));
    tupleBufferPointer.ref = Nautilus::Tracing::ValueRef(INT32_MAX, 0, IR::Types::StampFactory::createAddressStamp());
    auto executionTrace = Nautilus::Tracing::traceFunctionWithReturn([&]() {
        return getNumberOfTuples(tupleBufferPointer);
    });
    CompilationOptions options;
    options.setProxyInlining(true);
    auto result = prepare(executionTrace, options);
    auto function = result->getInvocableMember<uint64_t, uint8_t*>("execute");
    
    // Clean proxy functions issue: 3710
    // Todo (#3709) Right now we can only statically check one file, since we cannot configure the LLVM optimizer 
    // lambda function. With #3709 we change this. We should then write the generated code with inlined proxy functions
    // to files that we name using timestamps. These files should also be deleted again, at the end of the test.
    // -> avoid reading an old file (check if file already exists at the beginning of the test)
    // -> avoid reading a non-existing file (assert that file exists after code generation)
    // -> avoid accumulating test files (delete file with generated code at the end o the test)
    // -> potentially, we also test that inlining does not happen with optimization set to O0
    std::string line;
    std::ifstream infile( options.getProxyInliningPath() + "generated.ll");
    NES_ASSERT2_FMT(infile.peek() != std::ifstream::traits_type::eof(), "No proxy file was generated.");
    // while (std::getline(std::string(PROXY_FUNCTIONS_RESULT_DIR) + "generated.ll", line))
    bool foundExecute = false;
    while (std::getline(infile, line)) {  
        if(!foundExecute) {
            if(line.find("@execute") != std::string::npos) {
                foundExecute = true;
            }
        } else {
            // The generated execute function should not contain any tail calls.
            NES_ASSERT2_FMT(line.find("@tail call") == std::string::npos, "execute contained a tail call even though \
            all tail calls should have been removed via proxy function inlining.");
            // Check if we reached the end of the execute function.
            if(line == "}") {
                break;
            }
        }
    }
    NES_DEBUG("Function result: " << function((uint8_t*) std::addressof(tupleBuffer)));
}

// Tests all registered compilation backends.
auto pluginNames = Backends::CompilationBackendRegistry::getPluginNames();
INSTANTIATE_TEST_CASE_P(testFunctionCalls,
                        ProxyFunctionInliningCompilationTest,
                        ::testing::ValuesIn(pluginNames.begin(), pluginNames.end()),
                        [](const testing::TestParamInfo<ProxyFunctionInliningCompilationTest::ParamType>& info) {
                            return info.param;
                        });

}// namespace NES::Nautilus