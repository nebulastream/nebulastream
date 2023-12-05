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

#ifndef NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_WINDOWS_SYNOPSES_COUNTMINSYNOPSISDESCRIPTOR_HPP_
#define NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_WINDOWS_SYNOPSES_COUNTMINSYNOPSISDESCRIPTOR_HPP_

#include <Operators/LogicalOperators/Windows/Synopses/WindowSynopsisDescriptor.hpp>

namespace NES::Experimental::Statistics {

class CountMinSynopsisDescriptor : public WindowSynopsisDescriptor {
  public:
    explicit CountMinSynopsisDescriptor(ExpressionNodePtr fieldName,
                                        double error,
                                        double probability,
                                        time_t windowSize,
                                        time_t slideFactor);

    virtual ~CountMinSynopsisDescriptor() = default;

    bool operator==(const CountMinSynopsisDescriptor& otherDesc);

    std::string toString() const;

    [[nodiscard]] bool equal(NodePtr const& rhs) const;

    [[nodiscard]] double getError() const;

    [[nodiscard]] double getProbability() const;

  private:
    double error;
    double probability;
};
}// namespace NES::Experimental::Statistics

#endif//NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_WINDOWS_SYNOPSES_COUNTMINSYNOPSISDESCRIPTOR_HPP_
