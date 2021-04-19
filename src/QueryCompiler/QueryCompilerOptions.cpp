#include <QueryCompiler/QueryCompilerOptions.hpp>

namespace NES {
namespace QueryCompilation {

bool QueryCompilerOptions::isOperatorFusionEnabled() { return operatorFusion; }

void QueryCompilerOptions::enableOperatorFusion() { this->operatorFusion = true; }

void QueryCompilerOptions::disableOperatorFusion() { this->operatorFusion = false; }

void QueryCompilerOptions::setNumSourceLocalBuffers(uint64_t num) {
    this->numSourceLocalBuffers = num;
}

uint64_t QueryCompilerOptions::getNumSourceLocalBuffers() {
    return numSourceLocalBuffers;
}

QueryCompilerOptionsPtr QueryCompilerOptions::createDefaultOptions() {
    auto options = QueryCompilerOptions();
    options.enableOperatorFusion();
    return std::make_shared<QueryCompilerOptions>(options);
}

}// namespace QueryCompilation
}// namespace NES