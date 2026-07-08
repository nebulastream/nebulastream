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
#include <Plans/LogicalPlan.hpp>
#include <folly/Synchronized.h>
#include <QueryId.hpp>
#include <QueryStatus.hpp>

namespace NES
{

enum LocalQueryManagementState
{
    RUNNING = 1,
    MISSING = 2,
    WAITING_FOR_STOP = 3,
    READY_FOR_RESTART = 4,
    //STOPPED = 5,
    FAILED = 6,
    COMPLETED = 7,
};

struct LocalQueryMonitoringState
{
    QueryId queryId;
    bool missing = false;
    std::optional<LocalQueryStatusSnapshot> latestSnapshot = std::nullopt;
    std::chrono::system_clock::time_point lastHeartbeat = std::chrono::system_clock::from_time_t(0);
    bool lastHeartbeatSuccessful = true;
};

class LocalQuery
{
private:
    folly::Synchronized<LocalQueryMonitoringState> monitoringState;
    Host host;
    LogicalPlan localPlan;
    LocalQueryManagementState managementState = RUNNING;

    friend class DistributedQuery;

public:
    Host getHost() const { return host; }

    QueryId getCurrentQueryId() const { return monitoringState.rlock()->queryId; }

    LogicalPlan& getLocalPlan() { return localPlan; }

    LocalQueryManagementState getManagementState() const { return managementState; }

    void heartbeat(bool successful);
    void updateSnapshot(LocalQueryStatusSnapshot& snapshot, QueryId& queryId);
    void markMissing(QueryId& queryId);

    bool isMissing() const;

    void waitForStop();

    bool checkMaybeRunning();
    bool checkMissing();
    bool checkStop();
    bool checkFailed();

    bool isReadyForCompletion();
    void setCompleted();

    void reset();
    void setNewQueryId(QueryId& queryId);


    std::expected<LocalQueryStatusSnapshot, Exception> createProvisionalStatusSnapshot() const;

    LocalQuery(Host host, QueryId queryId, LogicalPlan localPlan)
        : monitoringState(LocalQueryMonitoringState{.queryId = std::move(queryId)}), host(std::move(host)), localPlan(std::move(localPlan))
    {
    }

    LocalQuery(const LocalQuery&) = delete;
    LocalQuery& operator=(const LocalQuery&) = delete;

    LocalQuery(LocalQuery&& other) noexcept
        : monitoringState(other.monitoringState.copy())
        , host(std::move(other.host))
        , localPlan(std::move(other.localPlan))
        , managementState(other.managementState)
    {
    }

    LocalQuery& operator=(LocalQuery&& other) noexcept
    {
        if (this != &other)
        {
            monitoringState = other.monitoringState.copy();
            host = std::move(other.host);
            localPlan = std::move(other.localPlan);
            managementState = other.managementState;
        }

        return *this;
    }
};

}