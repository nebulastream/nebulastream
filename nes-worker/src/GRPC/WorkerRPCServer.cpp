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

#include <Expressions/ExpressionSerializationUtil.hpp>
#include <GRPC/WorkerRPCServer.hpp>
#include <Mobility/LocationProviders/LocationProvider.hpp>
#include <Mobility/ReconnectSchedulePredictors/ReconnectSchedulePredictor.hpp>
#include <Monitoring/MonitoringAgent.hpp>
#include <Monitoring/MonitoringPlan.hpp>
#include <Operators/Serialization/DecomposedQueryPlanSerializationUtil.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Reconfiguration/ReconfigurationMarker.hpp>
#include <Reconfiguration/ReconfigurationMarkerSerializationUtil.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Mobility/ReconnectPoint.hpp>
#include <nlohmann/json.hpp>
#include <utility>

namespace NES {

WorkerRPCServer::WorkerRPCServer(Runtime::NodeEnginePtr nodeEngine,
                                 Monitoring::MonitoringAgentPtr monitoringAgent,
                                 NES::Spatial::Mobility::Experimental::LocationProviderPtr locationProvider,
                                 NES::Spatial::Mobility::Experimental::ReconnectSchedulePredictorPtr trajectoryPredictor)
    : nodeEngine(std::move(nodeEngine)), statisticManager(this->nodeEngine->getStatisticManager()),
      monitoringAgent(std::move(monitoringAgent)), locationProvider(std::move(locationProvider)),
      trajectoryPredictor(std::move(trajectoryPredictor)) {
    NES_DEBUG("WorkerRPCServer::WorkerRPCServer()");
}

Status WorkerRPCServer::RegisterDecomposedQuery(ServerContext*,
                                                const RegisterDecomposedQueryRequest* request,
                                                RegisterDecomposedQueryReply* reply) {
    //TODO: This removes the const qualifier please check this @Ankit #4769
    auto decomposedQueryPlan = DecomposedQueryPlanSerializationUtil::deserializeDecomposedQueryPlan(
        (SerializableDecomposedQueryPlan*) &request->decomposedqueryplan());

    NES_DEBUG("WorkerRPCServer::RegisterQuery: got decomposed query plan with shared query Id: {} and decomposed query plan Id: "
              "{} plan={}",
              decomposedQueryPlan->getSharedQueryId(),
              decomposedQueryPlan->getDecomposedQueryId(),
              decomposedQueryPlan->toString());
    bool success = false;
    try {
        //check if the plan is reconfigured
        NES_ASSERT(decomposedQueryPlan->getState() != QueryState::MARKED_FOR_REDEPLOYMENT,
                   "Plan marked for redeployment should not be registered again");
        success = nodeEngine->registerDecomposableQueryPlan(decomposedQueryPlan, request->replaydata());
    } catch (std::exception& error) {
        NES_ERROR("Register query crashed: {}", error.what());
        success = false;
    }
    if (success) {
        NES_DEBUG("WorkerRPCServer::RegisterQuery: success");
        reply->set_success(true);
        return Status::OK;
    }
    NES_ERROR("WorkerRPCServer::RegisterQuery: failed");
    reply->set_success(false);
    return Status::CANCELLED;
}

Status WorkerRPCServer::UnregisterDecomposedQuery(ServerContext*,
                                                  const UnregisterDecomposedQueryRequest* request,
                                                  UnregisterDecomposedQueryReply* reply) {
    NES_DEBUG("WorkerRPCServer::UnregisterQuery: got request for {}", request->sharedqueryid());
    bool success = nodeEngine->unregisterDecomposedQueryPlan(SharedQueryId(request->sharedqueryid()),
                                                             DecomposedQueryId(request->decomposedqueryid()));
    if (success) {
        NES_DEBUG("WorkerRPCServer::UnregisterQuery: success");
        reply->set_success(true);
        return Status::OK;
    }
    NES_ERROR("WorkerRPCServer::UnregisterQuery: failed");
    reply->set_success(false);
    return Status::CANCELLED;
}

Status WorkerRPCServer::StartDecomposedQuery(ServerContext*,
                                             const StartDecomposedQueryRequest* request,
                                             StartDecomposedQueryReply* reply) {
    NES_DEBUG("WorkerRPCServer::StartQuery: got request for {}", request->sharedqueryid());
    NES_ERROR("Start query, count = {}", request->count())
    bool success = nodeEngine->startDecomposedQueryPlan(SharedQueryId(request->sharedqueryid()),
                                                        DecomposedQueryId(request->decomposedqueryid()), request->count());
    if (success) {
        NES_DEBUG("WorkerRPCServer::StartQuery: success");
        NES_ERROR("Start query success , count = {}", request->count())
        reply->set_success(true);
        return Status::OK;
    }
    NES_ERROR("WorkerRPCServer::StartQuery: failed");
    reply->set_success(false);
    return Status::CANCELLED;
}

Status
WorkerRPCServer::StopDecomposedQuery(ServerContext*, const StopDecomposedQueryRequest* request, StopDecomposedQueryReply* reply) {
    NES_DEBUG("WorkerRPCServer::StopQuery: got request for {}", request->sharedqueryid());
    auto terminationType = Runtime::QueryTerminationType(request->queryterminationtype());
    NES_ASSERT2_FMT(terminationType != Runtime::QueryTerminationType::Invalid, "Invalid termination type requested");
    bool success = nodeEngine->stopDecomposedQueryPlan(SharedQueryId(request->sharedqueryid()),
                                                       DecomposedQueryId(request->decomposedqueryid()),
                                                       terminationType);
    if (success) {
        NES_DEBUG("WorkerRPCServer::StopQuery: success");
        reply->set_success(true);
        return Status::OK;
    }
    NES_ERROR("WorkerRPCServer::StopQuery: failed");
    reply->set_success(false);
    return Status::CANCELLED;
}

Status WorkerRPCServer::AddReconfigurationMarker(ServerContext*,
                                                 const ReconfigurationMarkerRequest* request,
                                                 ReconfigurationMarkerReply*) {

    auto reconfigurationMarker = ReconfigurationMarker::create();
    auto sharedQueryId = SharedQueryId(request->sharedqueryid());
    auto decomposedQueryId = DecomposedQueryId(request->decomposedqueryid());
    auto serializableReconfigurationMarker = request->serializablereconfigurationmarker();
    ReconfigurationMarkerSerializationUtil::deserialize(serializableReconfigurationMarker, reconfigurationMarker);
    NES_INFO("Received reconfiguration marker");
    auto status = nodeEngine->addReconfigureMarker(sharedQueryId, decomposedQueryId, reconfigurationMarker, request->count()) ? Status::OK
                                                                                                            : Status::CANCELLED;
    return status;
}

Status WorkerRPCServer::RegisterMonitoringPlan(ServerContext*,
                                               const MonitoringRegistrationRequest* request,
                                               MonitoringRegistrationReply*) {
    try {
        NES_DEBUG("WorkerRPCServer::RegisterMonitoringPlan: Got request");
        std::set<Monitoring::MetricType> types;
        for (auto type : request->metrictypes()) {
            types.insert((Monitoring::MetricType) type);
        }
        Monitoring::MonitoringPlanPtr plan = Monitoring::MonitoringPlan::create(types);
        monitoringAgent->setMonitoringPlan(plan);
        return Status::OK;
    } catch (std::exception& ex) {
        NES_ERROR("WorkerRPCServer: Registering monitoring plan failed: {}", ex.what());
    }
    return Status::CANCELLED;
}

Status WorkerRPCServer::GetMonitoringData(ServerContext*, const MonitoringDataRequest*, MonitoringDataReply* reply) {
    try {
        NES_DEBUG("WorkerRPCServer: GetMonitoringData request received");
        auto metrics = monitoringAgent->getMetricsAsJson().dump();
        NES_DEBUG("WorkerRPCServer: Transmitting monitoring data: {}", metrics);
        reply->set_metricsasjson(metrics);
        return Status::OK;
    } catch (std::exception& ex) {
        NES_ERROR("WorkerRPCServer: Requesting monitoring data failed: {}", ex.what());
    }
    return Status::CANCELLED;
}

Status WorkerRPCServer::BeginBuffer(ServerContext*, const BufferRequest* request, BufferReply* reply) {
    NES_DEBUG("WorkerRPCServer::BeginBuffer request received");

    auto decomposedQueryId = DecomposedQueryId(request->decomposedqueryid());
    auto decomposedQueryVersion = DecomposedQueryPlanVersion(request->decomposedqueryversion());
    auto uniqueNetworkSinkDescriptorId = OperatorId(request->uniquenetworksinkdescriptorid());
    bool success = nodeEngine->bufferData(decomposedQueryId, decomposedQueryVersion, uniqueNetworkSinkDescriptorId);
    if (success) {
        NES_DEBUG("WorkerRPCServer::StopQuery: success");
        reply->set_success(true);
        return Status::OK;
    } else {
        NES_ERROR("WorkerRPCServer::StopQuery: failed");
        reply->set_success(false);
        return Status::CANCELLED;
    }
}
Status
WorkerRPCServer::StartBufferingOnAllSinks(ServerContext*, const StartBufferingRequest* request, StartBufferingReply* reply) {
    //auto success = nodeEngine->bufferOutgoingTuples(INVALID_WORKER_NODE_ID);
    // NES_ERROR("Stop sending data to node {}, buffer until connected to node {}", nodeEngine->getParentId(), request->parent());
//    NES_ERROR("Start buffering request received, count = {}", request->count());
    nodeEngine->setParentId(request->parent(), request->count());
    //    nodeEngine->tryApplyingNewDescriptors();
    auto success = true;
    if (success) {
        NES_DEBUG("WorkerRPCServer::StartBufferingOnAllSinks: success");
        reply->set_success(true);
        return Status::OK;
    } else {
        NES_ERROR("WorkerRPCServer::StartBufferingOnAllSinks: failed");
        reply->set_success(false);
        return Status::CANCELLED;
    }
}

Status
WorkerRPCServer::UpdateNetworkSink(ServerContext*, const UpdateNetworkSinkRequest* request, UpdateNetworkSinkReply* reply) {
    NES_DEBUG("WorkerRPCServer::Sink Reconfiguration request received");
    auto decomposedQueryId = DecomposedQueryId(request->decomposedqueryid());
    auto decomposedQueryVersion = DecomposedQueryPlanVersion(request->decomposedqueryversion());
    auto uniqueNetworkSinkDescriptorId = OperatorId(request->uniquenetworksinkdescriptorid());
    auto newNodeId = WorkerId(request->newnodeid());
    std::string newHostname = request->newhostname();
    uint32_t newPort = request->newport();

    bool success = nodeEngine->updateNetworkSink(newNodeId,
                                                 newHostname,
                                                 newPort,
                                                 decomposedQueryId,
                                                 decomposedQueryVersion,
                                                 uniqueNetworkSinkDescriptorId);
    if (success) {
        NES_DEBUG("WorkerRPCServer::UpdateNetworkSinks: success");
        reply->set_success(true);
        return Status::OK;
    } else {
        NES_ERROR("WorkerRPCServer::UpdateNetworkSinks: failed");
        reply->set_success(false);
        return Status::CANCELLED;
    }
}

Status WorkerRPCServer::GetLocation(ServerContext*, const GetLocationRequest* request, GetLocationReply* reply) {
    (void) request;
    NES_DEBUG("WorkerRPCServer received location request")
    if (!locationProvider) {
        NES_DEBUG("WorkerRPCServer: locationProvider not set, node doesn't have known location")
        //return an empty reply
        return Status::OK;
    }
    auto waypoint = locationProvider->getCurrentWaypoint();
    auto loc = waypoint.getLocation();
    auto protoWaypoint = reply->mutable_waypoint();
    if (loc.isValid()) {
        auto coord = protoWaypoint->mutable_geolocation();
        coord->set_lat(loc.getLatitude());
        coord->set_lng(loc.getLongitude());
    }
    if (waypoint.getTimestamp()) {
        protoWaypoint->set_timestamp(waypoint.getTimestamp().value());
    }
    return Status::OK;
}

Status WorkerRPCServer::ProbeStatistics(ServerContext*, const ProbeStatisticsRequest* request, ProbeStatisticsReply* reply) {

    // Building a C++ StatisticProbeRequest object
    auto probeExpression = ExpressionSerializationUtil::deserializeExpression(request->expression());
    Statistic::StatisticProbeRequest probeRequest(request->statistichash(),
                                                  Windowing::TimeMeasure(request->startts()),
                                                  Windowing::TimeMeasure(request->endts()),
                                                  Windowing::TimeMeasure(request->granularity()),
                                                  Statistic::ProbeExpression(probeExpression));

    // Getting all statistics that match the probe request
    auto allProbedStatisticValues = statisticManager->getStatistics(probeRequest);
    for (const auto& stat : allProbedStatisticValues) {
        auto protoStat = reply->add_statistics();
        protoStat->set_startts(stat.getStartTs().getTime());
        protoStat->set_endts(stat.getEndTs().getTime());
        protoStat->set_statisticvalue(stat.getValue());
    }

    return Status::OK;
}

Status WorkerRPCServer::SendCheckpointToSource(ServerContext*, const CheckPointList* request, CheckPointRespone* reply) {
    NES_ERROR("request to replay received at worker")
//    //todo
//    (void) request;
//    (void) reply;
    for (auto [id, seq] : request->checkpoints()) {
        NES_ERROR("notifying checkpoint id {}, seq {}", id, seq)
       nodeEngine->getQueryManager()->setSourceAck(id, seq);
    }
    reply->set_success(true);
    return Status::OK;
}
}// namespace NES
