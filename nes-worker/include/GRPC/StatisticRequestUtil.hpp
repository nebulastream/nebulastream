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

#ifndef NES_NES_WORKER_INCLUDE_GRPC_STATISTICREQUESTUTIL_HPP_
#define NES_NES_WORKER_INCLUDE_GRPC_STATISTICREQUESTUTIL_HPP_

#include <memory>

namespace NES {

class GRPCStatisticProbeRequest;
class GRPCStatisticDeleteRequest;

namespace Experimental::Statistics {
class StatisticProbeRequest;
class StatisticDeleteRequest;
}

/**
 * @brief A small utility class, that serializes and deserializes statistic requests from their native NES types to the
 * automatically generated grpc types
 */
class StatisticRequestUtil {
  public:
    /**
     * @brief writes a NES native probe request to a grpc generated probe request obj
     * @param statisticProbeRequest the original probe request
     * @param grpcProbeRequest the grpc object to which we wish to copy the original probe request
     */
    static void serializeProbeRequest(const Experimental::Statistics::StatisticProbeRequest& statisticProbeRequest,
                                      GRPCStatisticProbeRequest* grpcProbeRequest);

    /**
     * @brief takes a delivered grpcProbeRequest and writes it to our own StatisticProbeRequest
     * @param GRPCStatisticProbeRequest a auto generated grpc type to be sent over the network that carries the information of probe request
     * @return StatisticProbeRequest our own statistic probeRequest type
     */
    static Experimental::Statistics::StatisticProbeRequest& deserializeProbeRequest(const GRPCStatisticProbeRequest* grpcProbeRequest);

    /**
     * @brief writes a NES native delete request to a grpc generated delete request obj
     * @param statisticDeleteRequest the original delete request
     * @param GRPCStatisticDeleteRequest the grpc object to which we wish to copy the original delete request
     */
    static void serializeDeleteRequest(const Experimental::Statistics::StatisticDeleteRequest& statisticDeleteRequest,
                                       GRPCStatisticDeleteRequest* grpcDeleteRequest);

    /**
     * @brief takes a delivered grpcDeleteRequest and writes it to our own StatDeleteRequestType
     * @param GRPCStatisticDeleteRequest a auto generated grpc type to be sent over the network that carries the information of delete request
     * @return StatisticDeleteRequest our own statistic deleteRequest type
     */
    static Experimental::Statistics::StatisticDeleteRequest& deserializeDeleteRequest(const GRPCStatisticDeleteRequest* grpcDeleteRequest);
};
}

#endif//NES_NES_WORKER_INCLUDE_GRPC_STATISTICREQUESTUTIL_HPP_
