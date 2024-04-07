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

#ifndef NES_COMMON_INCLUDE_UTIL_LATENCY_SYNTHETICTYPEUTILITY_HPP_
#define NES_COMMON_INCLUDE_UTIL_LATENCY_SYNTHETICTYPEUTILITY_HPP_

#include <Util/Latency/SyntheticType.hpp>
#include <WorkerCoordinate.grpc.pb.h>

namespace NES::Synthetic::Util {

        /**
* @brief this class contains functions to convert a synthetic type enum to its equivalent protobuf type and vice versa
* as well as functions to convert the node type enum to/from string
*/
    class SyntheticTypeUtility {
      public:
        static Experimental::SyntheticType stringToNodeType(const std::string syntheticTypeString);

        static Experimental::SyntheticType protobufEnumToNodeType(NES::Synthetic::Protobuf::SyntheticType syntheticType);

        static std::string toString(Experimental::SyntheticType syntheticType);

        static NES::Synthetic::Protobuf::SyntheticType toProtobufEnum(Experimental::SyntheticType syntheticType);
    };

}// namespace NES::Synthetic::Util

#endif// NES_COMMON_INCLUDE_UTIL_MOBILITY_SYNTHETICTYPEUTILITY_HPP_


