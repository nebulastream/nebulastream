//
// Created by ls on 1/26/24.
//
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/Slicing/NLJOperatorHandlerSlicing.hpp>
#include <cassert>
#include <tuple>
template<size_t SubQueryPlanId>
NES::Runtime::Execution::OperatorHandler* getSharedOperatorHandler(int index);
template<>
NES::Runtime::Execution::OperatorHandler* getSharedOperatorHandler<7>(int index) {
    static NES::Runtime::Execution::Operators::NLJOperatorHandlerSlicing handler1(std::vector<uint64_t>{uint64_t(2), uint64_t(3)},
                                                                                  uint64_t(1),
                                                                                  uint64_t(500),
                                                                                  uint64_t(500),
                                                                                  uint64_t(56),
                                                                                  uint64_t(16),
                                                                                  uint64_t(4096),
                                                                                  uint64_t(4096));

    static NES::Runtime::Execution::Operators::NLJOperatorHandlerSlicing handler2(std::vector<uint64_t>{uint64_t(4), uint64_t(5)},
                                                                                  uint64_t(2),
                                                                                  uint64_t(500),
                                                                                  uint64_t(500),
                                                                                  uint64_t(16),
                                                                                  uint64_t(16),
                                                                                  uint64_t(4096),
                                                                                  uint64_t(4096));

    assert(index < 2 && "Called getSharedOperatorHandler with an out of bound index");
    switch (index) {
        case 0: return &handler1;
        case 1: return &handler2;
    }

    return nullptr;
}