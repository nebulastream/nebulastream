#include <TestUtils/AbstractCompilationBackendTest.hpp>

namespace NES::Nautilus {

std::unique_ptr<Nautilus::Backends::Executable> AbstractCompilationBackendTest::prepare(std::shared_ptr<Nautilus::Tracing::ExecutionTrace> executionTrace) {
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    NES_DEBUG(*executionTrace.get());
    auto ir = irCreationPhase.apply(executionTrace);
    std::cout << ir->toString() << std::endl;
    auto param = this->GetParam();
    auto& compiler = Backends::CompilationBackendRegistry::getPlugin(param);
    return compiler->compile(ir);
}

}// namespace NES::Nautilus