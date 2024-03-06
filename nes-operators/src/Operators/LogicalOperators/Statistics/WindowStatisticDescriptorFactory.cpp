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

#include <Operators/LogicalOperators/Statistics/CountMinDescriptor.hpp>
#include <Operators/LogicalOperators/Statistics/ReservoirSampleDescriptor.hpp>
#include <Operators/LogicalOperators/Statistics/WindowStatisticDescriptorFactory.hpp>

namespace NES::Experimental::Statistics {

WindowStatisticDescriptorPtr WindowStatisticDescriptorFactory::createCountMinDescriptor(const std::string& logicalSourceName,
                                                                                      const std::string& fieldName,
                                                                                      const std::string& timestampField,
                                                                                      uint64_t depth,
                                                                                      uint64_t windowSize,
                                                                                      uint64_t slideFactor,
                                                                                      uint64_t width) {
    return std::make_shared<CountMinDescriptor>(logicalSourceName,
                                                fieldName,
                                                timestampField,
                                                depth,
                                                windowSize,
                                                slideFactor,
                                                width);
}

WindowStatisticDescriptorPtr
WindowStatisticDescriptorFactory::createReservoirSampleDescriptor(const std::string& logicalSourceName,
                                                                                             const std::string& fieldName,
                                                                                             const std::string& timestampField,
                                                                                             uint64_t depth,
                                                                                             uint64_t windowSize,
                                                                                             uint64_t slideFactor) {
    return std::make_shared<ReservoirSampleDescriptor>(logicalSourceName,
                                                       fieldName,
                                                       timestampField,
                                                       depth,
                                                       windowSize,
                                                       slideFactor);
}

}// namespace NES::Experimental::Statistics