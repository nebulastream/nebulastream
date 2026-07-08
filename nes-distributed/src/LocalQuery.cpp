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

#include <LocalQuery.hpp>

namespace NES
{


bool LocalQuery::checkMaybeRunning()
{
    checkMissing();
    checkStop();
    return managementState == RUNNING || managementState == WAITING_FOR_STOP;
}

bool LocalQuery::isMissing() const
{
    return managementState == MISSING;
}

void LocalQuery::waitForStop()
{
    managementState = WAITING_FOR_STOP;
}

bool LocalQuery::checkMissing()
{
    auto monitor = monitoringState.rlock();
    if (monitor->missing)
    {
        if (managementState == RUNNING)
        {
            managementState = MISSING;
        }
        else if (managementState == WAITING_FOR_STOP)
        {
            managementState = READY_FOR_RESTART;
        }
        else
        {
            INVARIANT(false, "Query state should be either RUNNING or WAITING_FOR_STOP here");
        }
        return true;
    }
    return false;
}

bool LocalQuery::checkStop()
{
    auto monitor = monitoringState.rlock();
    if (monitor->latestSnapshot->state == QueryStatus::Stopped)
    {
        if (managementState == WAITING_FOR_STOP)
        {
            managementState = READY_FOR_RESTART;
        }
        else
        {
            INVARIANT(managementState == READY_FOR_RESTART, "Query state should be READY_FOR_RESTART here");
        }
        return true;
    }
    return false;
}

bool LocalQuery::checkFailed()
{
    if (managementState == FAILED)
    {
        return true;
    }
    else if (managementState == COMPLETED)
    {
        return false;
    }
    auto monitor = monitoringState.rlock();
    if (monitor->latestSnapshot && monitor->latestSnapshot.value().state == QueryStatus::Failed)
    {
        managementState = FAILED;
        return true;
    }
    return false;
}

bool LocalQuery::isReadyForCompletion()
{
    auto monitor = monitoringState.rlock();
    if (managementState == RUNNING && monitor->latestSnapshot && monitor->latestSnapshot.value().state == QueryStatus::Stopped)
    {
        return true;
    }
    return false;
}

void LocalQuery::setCompleted()
{
    managementState = COMPLETED;
}

void LocalQuery::reset()
{
    managementState = RUNNING;
    auto monitor = monitoringState.wlock();
    monitor->latestSnapshot = std::nullopt;
}

void LocalQuery::setNewQueryId(QueryId& queryId)
{
    auto monitor = monitoringState.wlock();
    monitor->queryId = queryId;
}

void LocalQuery::updateSnapshot(LocalQueryStatusSnapshot& snapshot, QueryId& queryId)
{
    auto monitor = monitoringState.wlock();
    if (queryId == monitor->queryId) /// snapshots of outdated epochs are discarded
    {
        monitor->latestSnapshot = snapshot;
    }
}

void LocalQuery::markMissing(QueryId& queryId)
{
    auto monitor = monitoringState.wlock();
    if (queryId == monitor->queryId) /// errors for outdated epochs are discarded
    {
        monitor->missing = true;
    }
}

void LocalQuery::heartbeat(bool successful)
{
    auto monitor = monitoringState.wlock();
    monitor->lastHeartbeatSuccessful = successful;
    if (successful)
    {
        monitor->lastHeartbeat = std::chrono::system_clock::now();
    }
}

std::expected<LocalQueryStatusSnapshot, Exception> LocalQuery::createProvisionalStatusSnapshot() const
{
    LocalQueryStatusSnapshot returnValue;
    auto monitor = monitoringState.rlock();
    returnValue.queryId = monitor->queryId;
    if (monitor->latestSnapshot)
    {
        returnValue.metrics = monitor->latestSnapshot->metrics;
    }

    if (managementState == COMPLETED)
    {
        returnValue.state = QueryStatus::Stopped;
    }
    else if (managementState == FAILED)
    {
        returnValue.state = QueryStatus::Failed;
    }
    else
    {
        returnValue.state = QueryStatus::Running;
    }

    return returnValue;
}

}