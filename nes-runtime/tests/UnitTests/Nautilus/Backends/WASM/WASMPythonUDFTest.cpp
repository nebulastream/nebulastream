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
#include <Nautilus/Backends/WASM/WASMRuntime.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <NesBaseTest.hpp>

namespace NES::Nautilus {

class WASMPythonUDFTest : public Testing::NESBaseTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("WASMPythonUDFTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup WASMPythonUDFTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down WASMPythonUDFTest test class."); }
};

const char* wat = "(module\n"
                  " (import \"cpython\" \"cpython\" (func $python (result i32)))\n"
                  " (import \"cpython\" \"prepare_args\" (func $prepare_args (param i32 i32)))\n"
                  " (memory $memory i64 1)\n"
                  " (export \"memory\" (memory $memory))\n"
                  " (export \"execute\" (func $execute))\n"
                  " (func $execute (result i32)\n"
                  "  i32.const 8\n"
                  "  i32.const 6\n"
                  "  call $prepare_args\n"
                  "  call $python\n"
                  "  return\n"
                  " )\n"
                  ")";

TEST_F(WASMPythonUDFTest, addPythonUDF) {
    auto wasmRuntime = std::make_unique<Backends::WASM::WASMRuntime>();
    auto result = wasmRuntime->run(341, const_cast<char*>(wat));
    ASSERT_EQ(14, result);
}

}// namespace NES::Nautilus