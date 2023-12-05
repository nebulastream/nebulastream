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

#ifndef NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_WINDOWS_SYNOPSES_WINDOWSYNOPSISDESCRIPTOR_HPP_
#define NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_WINDOWS_SYNOPSES_WINDOWSYNOPSISDESCRIPTOR_HPP_

#include <Common/DataTypes/DataType.hpp>
#include <Operators/LogicalOperators/LogicalUnaryOperatorNode.hpp>
#include <Operators/LogicalOperators/Windows/WindowingForwardRefs.hpp>
#include <Operators/OperatorForwardDeclaration.hpp>
#include <vector>

namespace NES::Experimental::Statistics {

class WindowSynopsisDescriptor {
  public:
    enum class SynopsisType : uint8_t { CountMin, HLL, DDSketch, RSample };

    WindowSynopsisDescriptor(ExpressionNodePtr fieldName,
                             SynopsisType synopsisType,
                             time_t windowSize,
                             time_t slideFactor);

    std::string toString() const;

    SynopsisType getType();

    virtual void addSynopsisSpecificFields(SchemaPtr schema) = 0;

    std::string getTypeAsString() const;

    ExpressionNodePtr getFieldName() const;

    time_t getWindowSize() const;

    time_t getSlideFactor() const;

  protected:
    ExpressionNodePtr fieldName;
    SynopsisType synopsisType;
    time_t windowSize;
    time_t slideFactor;
};
}// namespace NES::Experimental::Statistics

#endif//NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_WINDOWS_SYNOPSES_WINDOWSYNOPSISDESCRIPTOR_HPP_
