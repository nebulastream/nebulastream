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

#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/CompilationRequest.hpp>
#include <Compiler/CompilationResult.hpp>
#include <Compiler/DynamicObject.hpp>
#include <Compiler/JITCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Compiler/SourceCode.hpp>
#include <Compiler/Util/File.hpp>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Compiler {

class JITCompilerTest : public testing::Test {
  public:
    uint64_t waitForCompilation = 10;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::setupLogging("JITCompilerTest.log", NES::LOG_DEBUG);
        std::cout << "Setup JITCompilerTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        std::cout << "Setup JITCompilerTest test case." << std::endl;
        auto cppCompiler = CPPCompiler::create();
        auto compilerBuilder = JITCompilerBuilder();
        compilerBuilder.registerLanguageCompiler(cppCompiler);
        compiler = compilerBuilder.build();
    }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down JITCompilerTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down JITCompilerTest test class." << std::endl; }

    std::shared_ptr<JITCompiler> compiler;
};

/**
 * @brief This test compiles a test CPP File
 */
TEST_F(JITCompilerTest, compileCppCode) {
    auto f = File("../tests/test_data/test_sourceCode/test1.cpp");
    auto content = f.read();
    auto sourceCode = std::make_unique<SourceCode>("cpp", content);
    auto request = CompilationRequest::create(std::move(sourceCode), "test_1", false, false, true, true);

    auto result = compiler->compile(std::move(request));
    result.wait_for(std::chrono::seconds(waitForCompilation));

    auto compilationResult = result.get();
    auto mulFunction = compilationResult.getDynamicObject()->getInvocableMember<int (*)(int, int)>("_Z3mulii");
    auto mulRes = mulFunction(10, 10);
    ASSERT_EQ(mulRes, 100);
    NES_DEBUG("CompilationTime:" << compilationResult.getCompilationTime());
}

}// namespace NES::Compiler