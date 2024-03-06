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

#ifndef NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICS_WINDOWSTATISTICDESCRIPTORFACTORY_HPP_
#define NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICS_WINDOWSTATISTICDESCRIPTORFACTORY_HPP_

#include <memory>

namespace NES::Experimental::Statistics {

class WindowStatisticDescriptor;
using WindowStatisticDescriptorPtr = std::shared_ptr<WindowStatisticDescriptor>;

class WindowStatisticDescriptorFactory {
  public:
    static WindowStatisticDescriptorPtr createCountMinDescriptor(const std::string& logicalSourceName,
                                                                 const std::string& fieldName,
                                                                 const std::string& timestampField,
                                                                 uint64_t depth,
                                                                 uint64_t windowSize,
                                                                 uint64_t slideFactor,
                                                                 uint64_t width);

    static WindowStatisticDescriptorPtr createReservoirSampleDescriptor(const std::string& logicalSourceName,
                                                                        const std::string& fieldName,
                                                                        const std::string& timestampField,
                                                                        uint64_t depth,
                                                                        uint64_t windowSize,
                                                                        uint64_t slideFactor);
};
}// namespace NES::Experimental::Statistics

#endif//NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICS_WINDOWSTATISTICDESCRIPTORFACTORY_HPP_
