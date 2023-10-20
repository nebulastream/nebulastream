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

#ifndef NES_NES_CORE_INCLUDE_STATISTICS_REQUESTS_REQUESTPARAMOBJ_HPP_
#define NES_NES_CORE_INCLUDE_STATISTICS_REQUESTS_REQUESTPARAMOBJ_HPP_

#include <string>
#include <memory>
#include <vector>

namespace NES {

namespace Experimental::Statistics {
/**
 * @brief The abstract class from which all requests inherit
 */
class RequestParamObj {
  public:
    RequestParamObj(const std::string& logicalSourceName,
                    const std::string& fieldName,
                    const std::string& createExpression)
        : logicalSourceName(logicalSourceName), fieldName(fieldName), createExpression(createExpression) {}

    /**
     * @return returns the logicalStreamName of the request
     */
    std::string getLogicalSourceName() const {
        return logicalSourceName;
    }

    /**
     * @return returns the fieldName of the request
     */
    std::string getFieldName() const {
        return fieldName;
    }

    /**
     * @return returns the expression, defining which stats to create, probe/query, or delete
     */
    std::string getCreateExpression() const {
        return createExpression;
    }

  private:
    std::string logicalSourceName;
    std::vector<std::string> physicalSourceNames;
    std::string fieldName;
    std::string createExpression;
};
}

}

#endif //NES_NES_CORE_INCLUDE_STATISTICS_REQUESTS_REQUESTPARAMOBJ_HPP_
