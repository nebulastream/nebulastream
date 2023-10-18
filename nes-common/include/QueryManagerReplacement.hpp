//
// Created by ls on 07.09.23.
//

#ifndef NES_QUERYMANAGERREPLACEMENT_HPP
#define NES_QUERYMANAGERREPLACEMENT_HPP

#include "Runtime/Execution/ExecutablePipeline.hpp"
#include "Runtime/ExecutionResult.hpp"
#include "Runtime/ReconfigurationMessage.hpp"
#include "Sinks/Mediums/SinkMedium.hpp"
#include <Runtime/RuntimeForwardRefs.hpp>

namespace NES::Unikernel::Querymanager {
namespace detail {
template<class... Ts>
struct overloaded : Ts... {
    using Ts::operator()...;
};
template<class... Ts>
overloaded(Ts...) -> overloaded<Ts...>;
}// namespace detail

[[maybe_unused]] static bool forwardTupleBuffer(Runtime::Execution::SuccessorExecutablePipeline successor,
                                                Runtime::TupleBuffer& buffer,
                                                Runtime::WorkerContext& workerContext) {
    return std::visit(detail::overloaded{[&buffer, &workerContext](const DataSinkPtr& sink) {
                                             return sink->writeData(buffer, workerContext);
                                         },
                                         [&buffer, &workerContext](const Runtime::Execution::ExecutablePipelinePtr& pipeline) {
                                             auto result = pipeline->execute(buffer, workerContext);
                                             if (result != ExecutionResult::Ok) {
                                                 NES_FATAL_ERROR("Pipeline execution failed");
                                             }
                                             return true;
                                         }},
                      successor);
}
[[maybe_unused]] static Runtime::ReconfigurablePtr getReconfigurable(Runtime::Execution::SuccessorExecutablePipeline successor) {
    return std::visit(detail::overloaded{[](const DataSinkPtr& sink) {
                                             return std::static_pointer_cast<Runtime::Reconfigurable>(sink);
                                         },
                                         [](const Runtime::Execution::ExecutablePipelinePtr& pipeline) {
                                             return std::static_pointer_cast<Runtime::Reconfigurable>(pipeline);
                                         }},
                      successor);
}
[[maybe_unused]] static void forwardReconfiguration(Runtime::Execution::SuccessorExecutablePipeline successor,
                                                    Runtime::ReconfigurationMessage& message,
                                                    Runtime::WorkerContext& workerContext) {
    return std::visit(detail::overloaded{[&message, &workerContext](const DataSinkPtr& sink) {
                                             return sink->reconfigure(message, workerContext);
                                         },
                                         [&message, &workerContext](const Runtime::Execution::ExecutablePipelinePtr& pipeline) {
                                             return pipeline->reconfigure(message, workerContext);
                                         }},
                      successor);
}
[[maybe_unused]] static QuerySubPlanId getSubPlanId(Runtime::Execution::SuccessorExecutablePipeline successor) {
    return std::visit(detail::overloaded{[](const DataSinkPtr& sink) {
                                             return sink->getParentPlanId();
                                         },
                                         [](const Runtime::Execution::ExecutablePipelinePtr& pipeline) {
                                             return pipeline->getQuerySubPlanId();
                                         }},
                      successor);
}
}// namespace NES::Unikernel::Querymanager
#endif//NES_QUERYMANAGERREPLACEMENT_HPP
