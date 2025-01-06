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
#include <GRPC/NesRPCClient.hpp>
#include <Runtime/NodeEngine.hpp>

namespace NES {

NesRPCClient::NesRPCClient(const std::string& workerAddress) {
    auto channel = grpc::CreateChannel(workerAddress, grpc::InsecureChannelCredentials());
    stub = WorkerRPCService::NewStub(channel);
}

bool NesRPCClient::getLoadStatistics(std::map<DecomposedQueryId, QueryPreviousMetrics>& outStats) {

    LoadStatsRequest request;
    LoadStatsReply reply;
    grpc::ClientContext context;

    auto status = stub->GetLoadStatistics(&context, request, &reply);
    if (!status.ok()) {
        return false;
    }

    for (auto& entry : reply.stats()) {
        DecomposedQueryId dqid(entry.subqueryid());
        QueryPreviousMetrics metrics = {
            entry.tasks(),
            entry.tuples(),
            entry.buffers(),
            entry.latencysum(),
            entry.queuesum(),
            entry.availableglobalbuffersum(),
            entry.availablefixedbuffersum()
        };
        outStats[dqid] = metrics;
    }

    return true;
}

bool NesRPCClient::performResourceHandshake(WorkerId workerId, uint64_t memoryCapacity) {
    ResourceHandshakeRequest request;
    request.set_workerid(workerId.getRawValue());
    request.set_memorycapacity(memoryCapacity);

    ResourceHandshakeReply reply;
    grpc::ClientContext context;
    auto status = stub->PerformResourceHandshake(&context, request, &reply);
    return status.ok() && reply.success();
}

} // namespace NES
