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

#ifndef NES_NES_CORE_INCLUDE_GRPC_STATREQUESTCOPYING_STATREQUESTUTIL_HPP_
#define NES_NES_CORE_INCLUDE_GRPC_STATREQUESTCOPYING_STATREQUESTUTIL_HPP_

#include <memory>

namespace NES {

class GRPCStatProbeRequest;
class GRPCStatDeleteRequest;

namespace Experimental::Statistics {
class StatProbeRequest;
class StatDeleteRequest;
}

/**
 * @brief A small utility class, that serializes and desiralizes stat requests from their native NES types to the
 * automatically generated grpc types
 */
class StatRequestUtil {
  public:
    /**
     * @brief writes a NES native probe request to a grpc generated probe request obj
     * @param probeRequestParamObjPtr the original probe request
     * @param grpcStatProbeRequest the grpc object to which we wish to copy the original probe request
     */
    static void serializeProbeRequest(const Experimental::Statistics::StatProbeRequest& statProbeRequest,
                                      GRPCStatProbeRequest* grpcProbeRequest);

    /**
     * @brief takes a delivered grpcProbeRequest and writes it to our own StatProbeRequestType
     * @param grpcProbeRequest a auto generated grpc type to be sent over the network that carries the information of probe request
     * @return StatProbeRequest
     */
    static Experimental::Statistics::StatProbeRequest& deserializeProbeRequest(const GRPCStatProbeRequest* grpcProbeRequest);

    /**
     * @brief writes a NES native delete request to a grpc generated delete request obj
     * @param probeRequestParamObjPtr the original delete request
     * @param grpcStatDeleteRequest the grpc object to which we wish to copy the original delete request
     */
    static void serializeDeleteRequest(const Experimental::Statistics::StatDeleteRequest& statDeleteRequest,
                                       GRPCStatDeleteRequest* grpcDeleteRequest);

    /**
     * @brief takes a delivered grpcDeleteRequest and writes it to our own StatDeleteRequestType
     * @param grpcDeleteRequest a auto generated grpc type to be sent over the network that carries the information of delete request
     * @return StatDeleteRequest
     */
    static Experimental::Statistics::StatDeleteRequest& deserializeDeleteRequest(const GRPCStatDeleteRequest* grpcDeleteRequest);
};
}

#endif//NES_NES_CORE_INCLUDE_GRPC_STATREQUESTCOPYING_STATREQUESTUTIL_HPP_
