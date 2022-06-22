#include <Experimental/Trace/SymbolicExecution/SymbolicExecutionPath.hpp>

namespace NES::ExecutionEngine::Experimental::Trace {
void SymbolicExecutionPath::append(bool outcome, Tag& tag) { path.emplace_back(std::make_tuple(outcome, tag)); }

std::tuple<bool, Tag> SymbolicExecutionPath::operator[](uint64_t size) { return path[size]; }

uint64_t SymbolicExecutionPath::getSize() { return path.size(); };

}// namespace NES::ExecutionEngine::Experimental::Trace