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
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Util/Logger/Logger.hpp>
#include <babelfish.h>
#include <boost/preprocessor/stringize.hpp>
#include <cxxabi.h>
#include <dlfcn.h>
#include <execinfo.h>
#include <flounder/compiler.h>
#include <flounder/executable.h>
#include <flounder/program.h>
#include <flounder/statement.h>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Nautilus {

class FlounderTest : public Testing::NESBaseTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("FlounderTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup FlounderTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override { NES_INFO("Setup FlounderTest test case."); }

    /* Will be called before a test is executed. */
    void TearDown() override { NES_INFO("Tear down FlounderTest test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down FlounderTest test class."); }
};

TEST_F(FlounderTest, BFSimpleTest) {
    graal_isolatethread_t* thread = NULL;
    ASSERT_TRUE(graal_create_isolate(NULL, NULL, &thread) == 0);
    std::string sourceCode = "SourceCodeOfPipeline";
    auto* pipeline = init(thread, sourceCode.data());
    run(thread, pipeline);
    run(thread, pipeline);
    run(thread, pipeline);
    run(thread, pipeline);
    graal_tear_down_isolate(thread);
}

TEST_F(FlounderTest, flounderGenTest) {
    auto program = flounder::Program{};

    /// Register for the first argument and the final result.
    /// Names of the virtual registers are freely selectable.
    auto* arg0 = program.vreg("arg0");
    auto* result = program.vreg("result");

    /// Allocate the virtual registers. The registers are usable from here.
    program << program.request_vreg64(arg0) << program.request_vreg64(result);

    /// Read the first function argument and write into the "arg0" virtual register.
    program << program.get_arg0(arg0);

    /// Set result = 42
    program << program.mov(result, program.constant8(42));

    /// When arg0 is > 10, calculate result = result * arg0.
    /// Code in the scope will be executed when the flounder::If{}-condition applies.
    {
        auto if_ = flounder::If{program, flounder::IsGreater{arg0, program.constant64(10)}};

        program << program.imul(result, arg0);
    }

    /// Return the result to the caller.
    program << program.set_return(result);

    /// Free the virtual registers. The registers are not usable anymore.
    program << program.clear(arg0) << program.clear(result);

    const auto flounder_code = program.to_string();

    /// Compile the program.
    auto executable = flounder::Executable{};
    auto compiler = flounder::Compiler{/*do not optimize*/ false,
                                       /*collect the asm code to print later*/ true,
                                       /*do not collect asm instruction offsets*/ false};
    const auto is_compilation_successful = compiler.compile(program, executable);
    ASSERT_TRUE(is_compilation_successful);

    /// Execute with arg0 = 11.
    constexpr std::int64_t argument = 11;
    NES_INFO(" == Execute == ");
    NES_INFO("return = " << executable.execute<std::uint64_t>(argument));

    /// Print flounder and assembly code.
    NES_INFO("\n == Flounder Code == \n" << flounder_code);
    if (executable.code().has_value()) {
        NES_INFO("\n == Assembly Code == \n" << executable.code().value());
    }
}

TEST_F(FlounderTest, flounderGenTestBlocks) {
    auto program = flounder::Program{};

    /// Register for the first argument and the final result.
    /// Names of the virtual registers are freely selectable.
    auto* arg0 = program.vreg("arg0");
    auto* result = program.vreg("result");

    /// Allocate the virtual registers. The registers are usable from here.
    program << program.request_vreg64(arg0) << program.request_vreg64(result);

    /// Read the first function argument and write into the "arg0" virtual register.
    program << program.get_arg0(arg0);

    /// Set result = 42
    program << program.mov(result, program.constant8(42));

    program << program.cmp(arg0, program.constant64(10));
    auto trueCaseL = program.label("trueCase");
    auto falseCaseL = program.label("falseCase");
    program << program.jg(trueCaseL);
    program << program.jmp(falseCaseL);
    program << program.section(trueCaseL);
    program << program.add(result, arg0);
    program << program.jmp(falseCaseL);
    program << program.section(falseCaseL);

    /// When arg0 is > 10, calculate result = result * arg0.
    /// Code in the scope will be executed when the flounder::If{}-condition applies.
    // {
    //     auto if_ = flounder::If{program, flounder::IsGreater{arg0, program.constant64(10)}};

    //     program << program.imul(result, arg0);
    // }

    /// Return the result to the caller.
    program << program.set_return(result);

    /// Free the virtual registers. The registers are not usable anymore.
    program << program.clear(arg0) << program.clear(result);

    const auto flounder_code = program.to_string();

    /// Compile the program.
    auto executable = flounder::Executable{};
    auto compiler = flounder::Compiler{/*do not optimize*/ false,
                                       /*collect the asm code to print later*/ true,
                                       /*do not collect asm instruction offsets*/ false};
    const auto is_compilation_successful = compiler.compile(program, executable);
    ASSERT_TRUE(is_compilation_successful);

    /// Execute with arg0 = 11.
    constexpr std::int64_t argument = 11;
    NES_INFO(" == Execute == ");
    NES_INFO("return = " << executable.execute<std::uint64_t>(argument));

    /// Print flounder and assembly code.
    NES_INFO("\n == Flounder Code == \n" << flounder_code);
    if (executable.code().has_value()) {
        NES_INFO("\n == Assembly Code == \n" << executable.code().value());
    }
}

}// namespace NES::Nautilus