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

#ifndef NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTIC_WINDOWSTATISTICSDESCRIPTOR_HPP_
#define NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTIC_WINDOWSTATISTICSDESCRIPTOR_HPP_

#include <memory>

namespace NES {

class Schema;
using SchemaPtr = std::shared_ptr<Schema>;

namespace Experimental::Statistics {

class WindowStatisticDescriptor {
  public:
    virtual bool operator==(WindowStatisticDescriptor& statisticsDescriptor) = 0;
    virtual void addStatisticFields(SchemaPtr schema) = 0;
    virtual ~WindowStatisticDescriptor() = default;
};
}
}
#endif//NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTIC_WINDOWSTATISTICSDESCRIPTOR_HPP_
