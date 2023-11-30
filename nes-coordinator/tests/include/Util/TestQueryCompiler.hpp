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
#ifndef NES_COORDINATOR_TESTS_INCLUDE_UTIL_TESTQUERYCOMPILER_HPP_
#define NES_COORDINATOR_TESTS_INCLUDE_UTIL_TESTQUERYCOMPILER_HPP_
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <QueryCompiler/NautilusQueryCompiler.hpp>
#include <QueryCompiler/Phases/DefaultPhaseFactory.hpp>
#include <QueryCompiler/Phases/Translations/DataSinkProvider.hpp>
#include <QueryCompiler/Phases/Translations/DefaultDataSourceProvider.hpp>
#include <QueryCompiler/Phases/Translations/LowerToExecutableQueryPlanPhase.hpp>
#include <QueryCompiler/QueryCompilationRequest.hpp>
#include <QueryCompiler/QueryCompilationResult.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Util/TestPhaseProvider.hpp>
#include <utility>

namespace NES::TestUtils {

/// utility method necessary if one wants to write a test that uses a mocked sink using a test sink descriptor
inline QueryCompilation::QueryCompilerPtr createTestQueryCompiler(
    QueryCompilation::QueryCompilerOptionsPtr options = QueryCompilation::QueryCompilerOptions::createDefaultOptions()) {
    auto phaseProvider = std::make_shared<TestPhaseProvider>();
    auto cppCompiler = Compiler::CPPCompiler::create();
    return QueryCompilation::NautilusQueryCompiler::create(options, phaseProvider);
}

}// namespace NES::TestUtils

#endif  // NES_COORDINATOR_TESTS_INCLUDE_UTIL_TESTQUERYCOMPILER_HPP_
