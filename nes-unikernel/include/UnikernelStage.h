//
// Created by ls on 09.09.23.
//

#ifndef NES_UNIKERNELSTAGE_H
#define NES_UNIKERNELSTAGE_H
#include <cstdint>
#include <cstddef>
#include <type_traits>
#include <concepts>

namespace NES::Runtime {
namespace Execution {
class OperatorHandler;
}
class WorkerContext;
class TupleBuffer;
}// namespace NES::Runtime

namespace NES::Unikernel {
class UnikernelPipelineExecutionContext;
template<typename T>
concept UnikernelStage = requires(T a) {
    { T::StageId } -> std::same_as<const size_t&>;
    { T::getOperatorHandler(std::declval<int>()) } -> std::same_as<NES::Runtime::Execution::OperatorHandler*>;
    T::execute(std::declval<NES::Unikernel::UnikernelPipelineExecutionContext&>(),
               std::declval<NES::Runtime::WorkerContext*>(),
               std::declval<NES::Runtime::TupleBuffer&>());
    T::setup(std::declval<NES::Unikernel::UnikernelPipelineExecutionContext&>(), std::declval<NES::Runtime::WorkerContext*>());
    T::terminate(std::declval<NES::Unikernel::UnikernelPipelineExecutionContext&>(),
                 std::declval<NES::Runtime::WorkerContext*>());
};

template<typename T>
concept IsEmitablePipelineStage = requires(T a) {
    { T::StageId } -> std::same_as<const size_t&>;
    T::execute(std::declval<NES::Runtime::TupleBuffer&>());
};

template<std::size_t STAGE_ID>
class Stage {
  public:
    constexpr static size_t StageId = STAGE_ID;
    static NES::Runtime::Execution::OperatorHandler* getOperatorHandler(int index);
    static void execute(NES::Unikernel::UnikernelPipelineExecutionContext& context,
                        NES::Runtime::WorkerContext* workerContext,
                        NES::Runtime::TupleBuffer& buffer);
    static void setup(NES::Unikernel::UnikernelPipelineExecutionContext& context, NES::Runtime::WorkerContext* workerContext);
    static void terminate(NES::Unikernel::UnikernelPipelineExecutionContext& context, NES::Runtime::WorkerContext* workerContext);
};
};    // namespace NES::Unikernel
#endif//NES_UNIKERNELSTAGE_H
