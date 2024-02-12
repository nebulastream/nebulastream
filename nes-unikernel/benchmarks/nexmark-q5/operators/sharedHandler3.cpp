#include <Execution/Operators/Streaming/Join/NestedLoopJoin/Slicing/NLJOperatorHandlerSlicing.hpp>
template<size_t SubQueryPlanId>
NES::Runtime::Execution::OperatorHandler* getSharedOperatorHandler(int index);
template<>
NES::Runtime::Execution::OperatorHandler* getSharedOperatorHandler<3>(int index) {
static NES::Runtime::Execution::Operators::NLJOperatorHandlerSlicing handler0(std::vector<uint64_t>{uint64_t(2),uint64_t(3)}, uint64_t(1), uint64_t(10000), uint64_t(2000), uint64_t(32), uint64_t(24), uint64_t(4096), uint64_t(4096));
switch(index) {
case 0:
	return &handler0;
default:
	assert(false && "Bad SharedOperatorHandler index");
	return nullptr;}}