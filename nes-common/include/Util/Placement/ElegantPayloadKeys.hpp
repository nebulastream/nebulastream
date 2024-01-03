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

#ifndef NES_COMMON_INCLUDE_ELEGANT_ELEGANTPAYLOADKEYS_HPP_
#define NES_COMMON_INCLUDE_ELEGANT_ELEGANTPAYLOADKEYS_HPP_

#include <string>

namespace NES {

//Query payload constants
const std::string OPERATOR_GRAPH_KEY = "operatorGraph";
const std::string OPERATOR_ID_KEY = "operatorId";
const std::string CHILDREN_KEY = "children";
const std::string CONSTRAINT_KEY = "constraint";
const std::string INPUT_DATA_KEY = "inputData";
const std::string JAVA_UDF_FIELD_KEY = "javaUdfField";

//Topology payload constants
const std::string DEVICE_ID_KEY = "deviceID";
const std::string DEVICES_KEY = "devices";
const std::string NODE_ID_KEY = "nodeId";
const std::string NODE_TYPE_KEY = "nodeType";

//Network delay payload constants
const std::string NETWORK_DELAYS_KEY = "networkDelays";
const std::string LINK_ID_KEY = "linkID";
const std::string TRANSFER_RATE_KEY = "transferRate";
const std::string TIME_WEIGHT_KEY = "time_weight";

//Response payload constants
const std::string AVAILABLE_NODES_KEY = "availNodes";
const std::string PLACEMENT_KEY = "placement";

}// namespace NES

#endif// NES_COMMON_INCLUDE_ELEGANT_ELEGANTPAYLOADKEYS_HPP_
