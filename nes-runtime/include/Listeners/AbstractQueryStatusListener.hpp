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
#pragma once

#include <Identifiers/Identifiers.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
struct AbstractQueryStatusListener
{
    virtual ~AbstractQueryStatusListener() noexcept = default;

    virtual bool logSourceTermination(QueryId queryId, OriginId sourceId, Runtime::QueryTerminationType) = 0;

    virtual bool logQueryFailure(QueryId queryId, Exception exception) = 0;

    virtual bool logQueryStatusChange(QueryId queryId, Runtime::Execution::QueryStatus Status) = 0;
};
}
