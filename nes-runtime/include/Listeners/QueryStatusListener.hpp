/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/
#ifndef NES_RUNTIME_INCLUDE_LISTENERS_QUERYSTATUSLISTENER_HPP_
#define NES_RUNTIME_INCLUDE_LISTENERS_QUERYSTATUSLISTENER_HPP_

#include <Identifiers/Identifiers.hpp>
#include <Runtime/Execution/ExecutableQueryPlanStatus.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <memory>
namespace NES {
class AbstractQueryStatusListener {
  public:
    virtual ~AbstractQueryStatusListener() noexcept = default;

    virtual bool canTriggerEndOfStream(SharedQueryId sharedQueryId,
                                       DecomposedQueryId decomposedQueryId,
                                       OperatorId sourceId,
                                       Runtime::QueryTerminationType) = 0;

    virtual bool notifySourceTermination(SharedQueryId sharedQueryId,
                                         DecomposedQueryId decomposedQueryId,
                                         OperatorId sourceId,
                                         Runtime::QueryTerminationType) = 0;

    virtual bool notifyQueryFailure(SharedQueryId sharedQueryId, DecomposedQueryId subQueryId, std::string errorMsg) = 0;

    virtual bool notifyQueryStatusChange(SharedQueryId sharedQueryId,
                                         DecomposedQueryId decomposedQueryId,
                                         Runtime::Execution::ExecutableQueryPlanStatus newStatus) = 0;

    virtual bool notifyEpochTermination(uint64_t timestamp, uint64_t querySubPlanId) = 0;

    virtual void notifyCheckpointToCoordinator(SharedQueryId, uint64_t) {}
};
using AbstractQueryStatusListenerPtr = std::shared_ptr<AbstractQueryStatusListener>;
}// namespace NES
#endif// NES_RUNTIME_INCLUDE_LISTENERS_QUERYSTATUSLISTENER_HPP_
