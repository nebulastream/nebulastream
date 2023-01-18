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
#ifndef NES_ENABLE_EXPERIMENTAL_EXECUTION_WAMR
#include "Nautilus/Backends/WASM/WAMRRuntime.hpp"
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <NesBaseTest.hpp>

namespace NES::Nautilus {

class WAMRExecutionTest : public Testing::NESBaseTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("WAMRExecutionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup WAMRExecutionTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down WAMRExecutionTest test class."); }
};

const char* wat = "(module\n"
                  " (memory $memory 1)\n"
                  " (export \"memory\" (memory $memory))\n"
                  " (export \"execute\" (func $execute))\n"
                  " (func $execute (result i32)\n"
                  "  i32.const 8\n"
                  "  i32.const 6\n"
                  "  i32.add\n"
                  "  return\n"
                  " )\n"
                  ")";

TEST_F(WAMRExecutionTest, addUDF) {
    auto wamrRuntime = std::make_unique<Backends::WASM::WAMRRuntime>();
    auto result = wamrRuntime->run(strlen(wat), const_cast<char*>(wat));
    //ASSERT_EQ(0, result);
}

}// namespace NES::Nautilus
#endif