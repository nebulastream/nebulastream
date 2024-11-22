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
#include <functional>
#include <memory>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>

namespace NES::Runtime
{
struct RunningQueryPlanNode;
class RunningSource;

class QueryLifetimeController
{
public:
    virtual ~QueryLifetimeController() = default;
    virtual void initializeSourceFailure(QueryId, OriginId, std::weak_ptr<RunningSource>, Exception exception) = 0;
    virtual void initializeSourceStop(QueryId, OriginId, std::weak_ptr<RunningSource>) = 0;
};

class WorkEmitter
{
public:
    using onComplete = std::function<void()>;
    using onFailure = std::function<void(Exception)>;

    virtual ~WorkEmitter() = default;

    virtual void emitWork(QueryId, const std::shared_ptr<RunningQueryPlanNode>&, Memory::TupleBuffer, onComplete, onFailure) = 0;
    virtual void emitPipelineStart(QueryId, const std::shared_ptr<RunningQueryPlanNode>&, onComplete, onFailure) = 0;
    virtual void emitPipelineStop(QueryId, std::unique_ptr<RunningQueryPlanNode>, onComplete, onFailure) = 0;
};
}
