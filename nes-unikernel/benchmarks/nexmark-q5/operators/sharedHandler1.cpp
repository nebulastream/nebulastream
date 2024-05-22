#include <Execution/Operators/Streaming/Join/NestedLoopJoin/Slicing/NLJOperatorHandlerSlicing.hpp>
template<size_t SubQueryPlanId>
NES::Runtime::Execution::OperatorHandler* getSharedOperatorHandler(int index);
template<>
NES::Runtime::Execution::OperatorHandler* getSharedOperatorHandler<1>(int index) {
static NES::Runtime::Execution::Operators::NLJOperatorHandlerSlicing handler0(std::vector<NES::OriginId>{NES::OriginId(2),NES::OriginId(3)}, NES::OriginId(1), uint64_t(10000), uint64_t(2000), NES::Nautilus::Interfaces::PagedVectorSize(4188,32,32), NES::Nautilus::Interfaces::PagedVectorSize(4188,24,24));
switch(index) {
case 0:
	return &handler0;
default:
	assert(false && "Bad SharedOperatorHandler index");
	return nullptr;}}