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

#include <BaseIntegrationTest.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/CompilationCache.hpp>
#include <Compiler/CompilationRequest.hpp>
#include <Compiler/CompilationResult.hpp>
#include <Compiler/DynamicObject.hpp>
#include <Compiler/JITCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Compiler/SourceCode.hpp>
#include <Compiler/Util/File.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>
namespace NES::Compiler {

class CompilationCacheTest : public Testing::BaseUnitTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("CompilationCacheTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup CompilationCacheTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        NES_DEBUG("Setup CompilationCacheTest test case.");
        auto cppCompiler = CPPCompiler::create();
        auto compilerBuilder = JITCompilerBuilder();
        compilerBuilder.registerLanguageCompiler(cppCompiler);
        compiler = compilerBuilder.build();
    }

    std::shared_ptr<JITCompiler> compiler;
};

/**
 * @brief This test compiles a test CPP File
 */
TEST_F(CompilationCacheTest, cacheSource) {
    CompilationCache compilationCache;
    auto sourceCode = SourceCode(Language::CPP, "TestSource");
    ASSERT_FALSE(compilationCache.contains(sourceCode));
    auto result = CompilationResult(std::shared_ptr<DynamicObject>(), Timer(""));
    compilationCache.insert(sourceCode, result);
    ASSERT_TRUE(compilationCache.contains(sourceCode));
}

}// namespace NES::Compiler