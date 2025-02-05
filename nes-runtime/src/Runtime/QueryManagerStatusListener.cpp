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

#include <Network/NetworkSink.hpp>
#include <Network/NetworkSource.hpp>
#include <Runtime/AsyncTaskExecutor.hpp>
#include <Runtime/Execution/ExecutablePipeline.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/ExecutableQueryPlan.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/HardwareManager.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/ThreadPool.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/Logger/Logger.hpp>
//#include <bits/socket.h>
#include <iostream>
#include <memory>
#include <utility>
#include <variant>
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>

namespace NES::Runtime {
void AbstractQueryManager::notifyQueryStatusChange(const Execution::ExecutableQueryPlanPtr& qep,
                                                   Execution::ExecutableQueryPlanStatus status) {
    NES_ASSERT(qep, "Invalid query plan object");
    if (status == Execution::ExecutableQueryPlanStatus::Finished) {
        for (const auto& source : qep->getSources()) {
            if (!std::dynamic_pointer_cast<Network::NetworkSource>(source)) {
                NES_ASSERT2_FMT(source->stop(Runtime::QueryTerminationType::Graceful),
                                "Cannot cleanup source " << source->getOperatorId());// just a clean-up op
            }
        }
        addReconfigurationMessage(qep->getSharedQueryId(),
                                  qep->getDecomposedQueryId(),
                                  qep->getDecomposedQueryVersion(),
                                  ReconfigurationMessage(qep->getSharedQueryId(),
                                                         qep->getDecomposedQueryId(),
                                                         qep->getDecomposedQueryVersion(),
                                                         ReconfigurationType::Destroy,
                                                         inherited1::shared_from_this()),
                                  false);

        queryStatusListener->notifyQueryStatusChange(qep->getSharedQueryId(),
                                                     qep->getDecomposedQueryId(),
                                                     Execution::ExecutableQueryPlanStatus::Finished);

    } else if (status == Execution::ExecutableQueryPlanStatus::ErrorState) {
        addReconfigurationMessage(qep->getSharedQueryId(),
                                  qep->getDecomposedQueryId(),
                                  qep->getDecomposedQueryVersion(),
                                  ReconfigurationMessage(qep->getSharedQueryId(),
                                                         qep->getDecomposedQueryId(),
                                                         qep->getDecomposedQueryVersion(),
                                                         ReconfigurationType::Destroy,
                                                         inherited1::shared_from_this()),
                                  false);

        queryStatusListener->notifyQueryStatusChange(qep->getSharedQueryId(),
                                                     qep->getDecomposedQueryId(),
                                                     Execution::ExecutableQueryPlanStatus::ErrorState);
    }
}

void AbstractQueryManager::notifySourceFailure(DataSourcePtr failedSource, const std::string reason) {
    NES_DEBUG("notifySourceFailure called for query id= {}  reason= {}", failedSource->getOperatorId(), reason);
    std::vector<Execution::ExecutableQueryPlanPtr> plansToFail;
    {
        std::unique_lock lock(queryMutex);
        plansToFail = sourceToQEPMapping[failedSource->getOperatorId()];
    }
    // we cant fail a query from a source because failing a query eventually calls stop on the failed query
    // this means we are going to call join on the source thread
    // however, notifySourceFailure may be called from the source thread itself, thus, resulting in a deadlock
    for (auto qepToFail : plansToFail) {
        auto future = asyncTaskExecutor->runAsync([this,
                                                   reason,
                                                   qepToFail = std::move(qepToFail)]() -> Execution::ExecutableQueryPlanPtr {
            NES_DEBUG("Going to fail query id={} subplan={}",
                      qepToFail->getDecomposedQueryId(),
                      qepToFail->getDecomposedQueryId());
            if (failExecutableQueryPlan(qepToFail)) {
                NES_DEBUG("Failed query id= {} subplan={}", qepToFail->getDecomposedQueryId(), qepToFail->getDecomposedQueryId());
                queryStatusListener->notifyQueryFailure(qepToFail->getSharedQueryId(), qepToFail->getDecomposedQueryId(), reason);
                return qepToFail;
            }
            return nullptr;
        });
    }
}

void AbstractQueryManager::notifyTaskFailure(Execution::SuccessorExecutablePipeline pipelineOrSink,
                                             const std::string& errorMessage) {

    DecomposedQueryId planId = INVALID_DECOMPOSED_QUERY_PLAN_ID;
    DecomposedQueryPlanVersion planVersion = INVALID_DECOMPOSED_QUERY_PLAN_VERSION;

    Execution::ExecutableQueryPlanPtr qepToFail;
    if (auto* pipe = std::get_if<Execution::ExecutablePipelinePtr>(&pipelineOrSink)) {
        planId = (*pipe)->getDecomposedQueryId();
        planVersion = (*pipe)->getDecomposedQueryVersion();
    } else if (auto* sink = std::get_if<DataSinkPtr>(&pipelineOrSink)) {
        planId = (*sink)->getParentPlanId();
        planVersion = (*sink)->getParentPlanVersion();
    }
    auto planIdWithVersion = DecomposedQueryIdWithVersion(planId, planVersion);
    {
        std::unique_lock lock(queryMutex);
        if (auto it = runningQEPs.find(planIdWithVersion); it != runningQEPs.end()) {
            qepToFail = it->second;
        } else {
            NES_WARNING("Cannot fail non existing sub query plan: {}.{}", planId, planVersion);
            return;
        }
    }
    auto future = asyncTaskExecutor->runAsync(
        [this, errorMessage](Execution::ExecutableQueryPlanPtr qepToFail) -> Execution::ExecutableQueryPlanPtr {
            NES_DEBUG("Going to fail query id= {} subplan={}.{}",
                      qepToFail->getSharedQueryId(),
                      qepToFail->getDecomposedQueryId(),
                      qepToFail->getDecomposedQueryVersion());
            if (failExecutableQueryPlan(qepToFail)) {
                NES_DEBUG("Failed query id= {}  subplan= {}.{}",
                          qepToFail->getSharedQueryId(),
                          qepToFail->getDecomposedQueryId(),
                          qepToFail->getDecomposedQueryVersion());
                queryStatusListener->notifyQueryFailure(qepToFail->getSharedQueryId(),
                                                        qepToFail->getDecomposedQueryId(),
                                                        errorMessage);
                return qepToFail;
            }
            return nullptr;
        },
        std::move(qepToFail));
}

void AbstractQueryManager::notifySourceCompletion(DataSourcePtr source, QueryTerminationType terminationType) {
    std::unique_lock lock(queryMutex);
    //Check if a QEP exists for the datasource for which completion notification received.
    const auto& operatorId = source->getOperatorId();
    // Shutting down all sources by notifying source termination
    for (auto& entry : sourceToQEPMapping[operatorId]) {
        if (entry->getStatus() != Execution::ExecutableQueryPlanStatus::Finished) {
            NES_TRACE("notifySourceCompletion operator id={} plan id={} subplan={}",
                      operatorId,
                      entry->getSharedQueryId(),
                      entry->getDecomposedQueryId());
            entry->notifySourceCompletion(source, terminationType);
            if (terminationType == QueryTerminationType::Graceful || terminationType == QueryTerminationType::Reconfiguration) {
                queryStatusListener->notifySourceTermination(entry->getSharedQueryId(),
                                                             entry->getDecomposedQueryId(),
                                                             operatorId,
                                                             terminationType);
            }
        } else {
            NES_TRACE("notifySourceCompletion: operator id={} plan id={} subplan={} is already finished",
                      operatorId,
                      entry->getSharedQueryId(),
                      entry->getDecomposedQueryId());
        }
    }
}

void AbstractQueryManager::notifyPipelineCompletion(DecomposedQueryId decomposedQueryId,
                                                    Execution::ExecutablePipelinePtr pipeline,
                                                    QueryTerminationType terminationType) {
    std::unique_lock lock(queryMutex);
    auto& qep = runningQEPs[DecomposedQueryIdWithVersion(decomposedQueryId, pipeline->getDecomposedQueryVersion())];
    NES_ASSERT2_FMT(qep, "invalid query plan for pipeline " << pipeline->getPipelineId());
    qep->notifyPipelineCompletion(pipeline, terminationType);
}

void AbstractQueryManager::notifySinkCompletion(DecomposedQueryId decomposedQueryId,
                                                DecomposedQueryPlanVersion decomposedQueryVersion,
                                                DataSinkPtr sink,
                                                QueryTerminationType terminationType) {
    std::unique_lock lock(queryMutex);
    auto& qep = runningQEPs[DecomposedQueryIdWithVersion(decomposedQueryId, decomposedQueryVersion)];
    NES_ASSERT2_FMT(qep,
                    "invalid decomposed query plan id and version" << decomposedQueryId << "." << decomposedQueryVersion
                                                                   << " for sink " << sink->toString());
    qep->notifySinkCompletion(sink, terminationType);
}

bool AbstractQueryManager::updatePlanVersion(DecomposedQueryIdWithVersion idWithVersion, DecomposedQueryPlanVersion newVersion) {
    std::unique_lock lock(queryMutex);

    auto planIterator = decomposeQueryToSourceIdMapping.find(idWithVersion);
    if (planIterator == decomposeQueryToSourceIdMapping.end()) {
        return false;
    }

    DecomposedQueryIdWithVersion newIdWithVersion(idWithVersion.id, newVersion);
    auto sourceIds = planIterator->second;
    decomposeQueryToSourceIdMapping[newIdWithVersion] = sourceIds;
    decomposeQueryToSourceIdMapping.erase(idWithVersion);

    auto runningPlanIteator = runningQEPs.find(idWithVersion);
    runningQEPs[newIdWithVersion] = runningPlanIteator->second;
    runningQEPs.erase(runningPlanIteator);

    return true;
}

void AbstractQueryManager::updateSourceToQepMapping(NES::OperatorId sourceid,
                                                    std::vector<Execution::ExecutableQueryPlanPtr> newPlans) {
    if (newPlans.size() > 1) {
        NES_ERROR("Source reuse is currently not supported in combination with source sharing");
        NES_NOT_IMPLEMENTED();
    }
    std::unique_lock lock(queryMutex);

    auto oldPlans = sourceToQEPMapping[sourceid];
    for (const auto& oldPlan : oldPlans) {
        decomposeQueryToSourceIdMapping.erase(
            DecomposedQueryIdWithVersion(oldPlan->getDecomposedQueryId(), oldPlan->getDecomposedQueryVersion()));
    }

    for (const auto& plan : newPlans) {
        decomposeQueryToSourceIdMapping[DecomposedQueryIdWithVersion(plan->getDecomposedQueryId(),
                                                                     plan->getDecomposedQueryVersion())]
            .push_back(sourceid);
    }
    sourceToQEPMapping[sourceid] = std::move(newPlans);
}

folly::Synchronized<TcpSourceInfo>::LockedPtr AbstractQueryManager::getTcpSourceInfo(std::string sourceName, std::string filePath) {
    std::unique_lock lock(tcpSourceMutex);
    if (tcpSourceInfos.contains(sourceName)) {
        auto locked =  tcpSourceInfos.at(sourceName).wlock();
//        NES_ASSERT(locked->port == std::stol(filePath))
        return tcpSourceInfos.at(sourceName).wlock();
    }

    uint64_t port = std::stol(filePath);
    // Create a TCP socket
    if (port != 0) {
        // Create a TCP socket
        int sockfd = socket(AF_INET, SOCK_STREAM, 0);
        if (sockfd == -1) {
            NES_ERROR("Could not open socket in source")
            perror("socket");
            NES_FATAL_ERROR("could not open socket in source");
        }
        struct timeval tv;
        tv.tv_sec = 1;
        tv.tv_usec = 0;
        setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof tv);

        // Specify the server address and port
        struct sockaddr_in server_addr;
        server_addr.sin_family = AF_INET;
        server_addr.sin_addr.s_addr = inet_addr("127.0.0.1");// Server IP address
        server_addr.sin_port = htons(port);

        // Connect to the server
        if (connect(sockfd, (struct sockaddr*) &server_addr, sizeof(server_addr)) == -1) {
            NES_ERROR("Could not connect to socket in source")
            perror("connect");
            ::close(sockfd);
            NES_FATAL_ERROR("could not open socket in source");
        }

        TcpSourceInfo info {
            port,
            sockfd,
            {},
            {},
            {},
        };

        tcpSourceInfos.insert({filePath, folly::Synchronized(info)});
        return tcpSourceInfos.at(filePath).wlock();
    }
//    NES_ERROR("Created new tcp descriptor {} for {}", sockfd, filePath);
    NES_FATAL_ERROR("invalid port");
    return tcpSourceInfos.at(filePath).wlock();
}
}// namespace NES::Runtime
