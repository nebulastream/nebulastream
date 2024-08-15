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
#ifndef NES_RUNTIME_INCLUDE_LISTENERS_ABSTRACTQUERYSTATUSLISTENER_HPP_
#define NES_RUNTIME_INCLUDE_LISTENERS_ABSTRACTQUERYSTATUSLISTENER_HPP_

#include <Exceptions/Exception.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <Runtime/QueryTerminationType.hpp>

namespace NES
{
class AbstractQueryStatusListener
{
public:
    virtual ~AbstractQueryStatusListener() noexcept = default;

    virtual bool canTriggerEndOfStream(QueryId queryId, OperatorId sourceId, Runtime::QueryTerminationType) = 0;

    virtual bool notifySourceTermination(QueryId queryId, OperatorId sourceId, Runtime::QueryTerminationType) = 0;

    virtual bool notifyQueryFailure(QueryId queryId, const Exception& exception) = 0;

    virtual bool notifyQueryStatusChange(QueryId queryId, Runtime::Execution::QueryStatus Status) = 0;

    virtual bool notifyEpochTermination(QueryId queryId, uint64_t timestamp) = 0;
};
} /// namespace NES
#endif /// NES_RUNTIME_INCLUDE_LISTENERS_ABSTRACTQUERYSTATUSLISTENER_HPP_
