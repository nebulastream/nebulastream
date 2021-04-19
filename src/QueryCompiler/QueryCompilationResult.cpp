#include <QueryCompiler/QueryCompilationResult.hpp>

namespace NES {
namespace QueryCompilation {

QueryCompilationResultPtr QueryCompilationResult::create() {
    return std::make_shared<QueryCompilationResult>();
}

}// namespace QueryCompilation
}// namespace NES