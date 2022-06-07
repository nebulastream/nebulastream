#include <Experimental/Trace/SymbolicExecution/SymbolicExecutionPath.hpp>


namespace NES::ExecutionEngine::Experimental::Trace {
void SymbolicExecutionPath::append(bool outcome, Tag& tag) { path.emplace_back(std::make_tuple(outcome, tag)); }

}