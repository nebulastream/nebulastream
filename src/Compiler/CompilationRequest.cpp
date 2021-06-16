
#include <Compiler/CompilationRequest.hpp>
#include <Compiler/SourceCode.hpp>
#include <memory>
namespace NES::Compiler {

CompilationRequest::CompilationRequest(std::unique_ptr<SourceCode> sourceCode,
                                       bool profileCompilation,
                                       bool profileExecution,
                                       bool optimizeCompilation,
                                       bool debug)
    : sourceCode(std::move(sourceCode)), profileCompilation(profileCompilation), profileExecution(profileExecution),
      optimizeCompilation(optimizeCompilation), debug(debug) {}

std::unique_ptr<CompilationRequest> CompilationRequest::create(std::unique_ptr<SourceCode> sourceCode,
                                                               bool profileCompilation,
                                                               bool profileExecution,
                                                               bool optimizeCompilation,
                                                               bool debug) {
    return std::make_unique<CompilationRequest>(std::move(sourceCode), profileCompilation, profileExecution, optimizeCompilation, debug);
};

bool CompilationRequest::enableOptimizations() const {
    return optimizeCompilation;
}

bool CompilationRequest::enableDebugging() const {
    return debug;
}

bool CompilationRequest::enableCompilationProfiling() const {
    return profileCompilation;
}

bool CompilationRequest::enableExecutionProfiling() const {
    return profileExecution;
}

const std::shared_ptr<SourceCode> CompilationRequest::getSourceCode() const {
    return sourceCode;
}

}// namespace NES::Compiler