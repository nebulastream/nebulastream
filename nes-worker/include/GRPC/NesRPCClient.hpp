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

#include <Identifiers/Identifiers.hpp>
#include <WorkerRPCService.grpc.pb.h>
#include <grpcpp/grpcpp.h>
#include <memory>
#include <string>

namespace NES {
struct QueryPreviousMetrics;

class NesRPCClient {
public:
    explicit NesRPCClient(const std::string& workerAddress);

    bool getLoadStatistics(std::map<DecomposedQueryId, QueryPreviousMetrics>& outStats);

    bool performResourceHandshake(WorkerId workerId, uint64_t memoryCapacity);


private:
    std::unique_ptr<WorkerRPCService::Stub> stub;
};

} // namespace NES
