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

#ifndef NES_NES_CORE_INCLUDE_STATISTICS_REQUESTS_CREATEREQUESTPARAMOBJ_HPP_
#define NES_NES_CORE_INCLUDE_STATISTICS_REQUESTS_CREATEREQUESTPARAMOBJ_HPP_

#include <string>
#include <memory>
#include <vector>

#include <Statistics/Requests/RequestParamObj.hpp>
#include <Statistics/StatCollectors/StatCollectorType.hpp>

namespace NES {
namespace Experimental::Statistics {

/**
 * @brief the inherited class that defines what is needed to build Statistics
 */
class CreateRequestParamObj : public RequestParamObj {
  public:
    CreateRequestParamObj(const std::string& logicalSourceName,
                         const std::string& fieldName,
                         const StatCollectorType statCollectorType)
        : RequestParamObj(logicalSourceName, fieldName, statCollectorType) {}
		};
	}
}

#endif //NES_NES_CORE_INCLUDE_STATISTICS_REQUESTS_CREATEEQUESTPARAMOBJ_HPP_
