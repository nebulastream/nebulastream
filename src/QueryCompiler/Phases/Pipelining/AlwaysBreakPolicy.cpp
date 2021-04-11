#include <QueryCompiler/Phases/Pipelining/AlwaysBreakPolicy.hpp>

namespace NES {
namespace QueryCompilation {
bool AlwaysBreakPolicy::isFusible(PhysicalOperators::PhysicalOperatorPtr) { return true; }
PipelineBreakerPolicyPtr AlwaysBreakPolicy::create() { return std::make_shared<AlwaysBreakPolicy>(); }
}// namespace QueryCompilation
}// namespace NES