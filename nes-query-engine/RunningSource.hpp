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
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Sources/SourceHandle.hpp>
#include <ErrorHandling.hpp>
#include "Interfaces.hpp"

namespace NES::Runtime
{
struct RunningQueryPlan;
struct RunningQueryPlanNode;


/// The Running Source is a wrapper around the SourceHandle. The Lifecycle of the RunningSource controls start/stop of the source handle.
/// The purpose of the running source is to create the emit function which and redirects external events towards the task queue, where one of
/// the WorkerThreads can handle them. We cannot allow that the SourceThread causes the RunningSource to be destroyed, which would cause a
/// deadlock. Additionally, the RunningSource prevents the SourceThread from accidentally triggering the successor pipeline termination, by
/// saving references to the successors. Only if both the SourceThread and the RunningSource are destroyed are successor pipelines terminated.
class RunningSource
{
public:
    /// Creates and starts the underlying source implementation. As long as the RunningSource is kept alive the source will run,
    /// once the last reference to the RunningSource is destroyed the source is stopped.
    static std::shared_ptr<RunningSource> create(
        QueryId queryId,
        std::unique_ptr<Sources::SourceHandle> source,
        std::vector<std::shared_ptr<RunningQueryPlanNode>> successors,
        std::function<void(std::vector<std::shared_ptr<RunningQueryPlanNode>>&&)> unregister,
        std::function<void(Exception)> unregisterWithError,
        QueryLifetimeController& controller,
        WorkEmitter& emitter);

    RunningSource(const RunningSource& other) = delete;
    RunningSource& operator=(const RunningSource& other) = delete;
    RunningSource(RunningSource&& other) noexcept = default;
    RunningSource& operator=(RunningSource&& other) noexcept = default;

    ~RunningSource();
    [[nodiscard]] OriginId getOriginId() const;

    void stop();
    void fail(Exception exception) const;

private:
    RunningSource(
        std::vector<std::shared_ptr<RunningQueryPlanNode>> successors,
        std::unique_ptr<Sources::SourceHandle> source,
        std::function<void(std::vector<std::shared_ptr<RunningQueryPlanNode>>&&)> unregister,
        std::function<void(Exception)> unregisterWithError);

    std::vector<std::shared_ptr<RunningQueryPlanNode>> successors;
    std::unique_ptr<Sources::SourceHandle> source;
    std::function<void(std::vector<std::shared_ptr<RunningQueryPlanNode>>&&)> unregister;
    std::function<void(Exception)> unregisterWithError;
};

}
