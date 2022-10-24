

#ifndef NES_NES_RUNTIME_TESTS_INCLUDE_TESTUTILS_ABSTRACTCOMPILATIONBACKENDTEST_HPP_
#define NES_NES_RUNTIME_TESTS_INCLUDE_TESTUTILS_ABSTRACTCOMPILATIONBACKENDTEST_HPP_

#include <Nautilus/Backends/CompilationBackend.hpp>
#include <Nautilus/Backends/Executable.hpp>
#include <Nautilus/Tracing/Phases/SSACreationPhase.hpp>
#include <Nautilus/Tracing/Phases/TraceToIRConversionPhase.hpp>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>

namespace NES::Nautilus {
/**
 * @brief This test tests execution of scala expression
 */
class AbstractCompilationBackendTest : public ::testing::WithParamInterface<std::string> {
  public:
    Nautilus::Tracing::SSACreationPhase ssaCreationPhase;
    Nautilus::Tracing::TraceToIRConversionPhase irCreationPhase;
    auto prepare(std::shared_ptr<Nautilus::Tracing::ExecutionTrace> executionTrace) {
        executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
        std::cout << *executionTrace.get() << std::endl;
        auto ir = irCreationPhase.apply(executionTrace);
        std::cout << ir->toString() << std::endl;
        auto param = this->GetParam();
        auto& compiler = Backends::CompilationBackendRegistry::getPlugin(param);
        return compiler->compile(ir);
    }
};
}// namespace NES::Nautilus

#endif//NES_NES_RUNTIME_TESTS_INCLUDE_TESTUTILS_ABSTRACTCOMPILATIONBACKENDTEST_HPP_
