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

#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/CompilationRequest.hpp>
#include <Compiler/CompilationCache.hpp>
#include <Compiler/CompilationResult.hpp>
#include <Compiler/DynamicObject.hpp>
#include <Compiler/JITCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Compiler/SourceCode.hpp>
#include <Compiler/Util/File.hpp>
#include <NesBaseTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>
namespace NES::Compiler {

class CompilationCacheTest : public Testing::TestWithErrorHandling<testing::Test> {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("CompilationCacheTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup CompilationCacheTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::TestWithErrorHandling<testing::Test>::SetUp();
        std::cout << "Setup CompilationCacheTest test case." << std::endl;
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
TEST_F(CompilationCacheTest, cacheSource) {
    CompilationCache compilationCache;
    auto sourceCode = SourceCode("cpp", "TestSource");
    ASSERT_FALSE(compilationCache.contains(sourceCode));
    auto result = CompilationResult(std::shared_ptr<DynamicObject>(), Timer(""));
    compilationCache.insert(sourceCode, result);
    ASSERT_TRUE(compilationCache.contains(sourceCode));

}

}// namespace NES::Compiler