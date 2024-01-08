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

#ifndef NES_NES_COMMON_INCLUDE_STATISTICFIELDIDENTIFIERS_HPP_
#define NES_NES_COMMON_INCLUDE_STATISTICFIELDIDENTIFIERS_HPP_

#include <cstdint>
#include <string>

namespace NES::Experimental::Statistics {

const std::string LOGICAL_SOURCE_NAME = "logicalSourceName";
const std::string PHYSICAL_SOURCE_NAME = "physicalSourceName";
const std::string WORKER_ID = "WorkerId";
const std::string FIELD_NAME = "fieldName";
const std::string OBSERVED_TUPLES = "observedTuples";
const std::string DEPTH = "depth";
const std::string START_TIME = "startTime";
const std::string END_TIME = "endTime";
const std::string DATA = "data";
const std::string WIDTH = "width";
}

#endif//NES_NES_COMMON_INCLUDE_STATISTICFIELDIDENTIFIERS_HPP_
