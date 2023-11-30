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

#ifndef NES_COORDINATOR_INCLUDE_PARSERS_NEBULAPSL_NEBULAPSLOPERATORNODE_HPP_
#define NES_COORDINATOR_INCLUDE_PARSERS_NEBULAPSL_NEBULAPSLOPERATORNODE_HPP_

#include <API/Query.hpp>
#include <list>
#include <queue>
#include <string>

namespace NES::Parsers {

/**
 * @brief This class defines the attributes and methods used by the PatternParsingService.
 * This enables the parsing of declarative patterns into NES queries.
 * Each operatorNode represents a node from the ANTLR AST tree with a unique identifier (id), pointers to the parent and child nodes
 * and specific attributes of the specific operator in order to create the query (tree).
 */

class NebulaPSLOperatorNode {
  public:
    //Constructors
    explicit NebulaPSLOperatorNode(int32_t id);
    // Getter and Setter
    int32_t getId() const;
    void setId(int32_t id);
    const std::string& getOperatorName() const;
    void setOperatorName(const std::string& operatorName);
    int32_t getRightChildId() const;
    void setRightChildId(int32_t rightChildId);
    int32_t getLeftChildId() const;
    void setLeftChildId(int32_t leftChildId);
    const std::pair<int, int>& getMinMax() const;
    void setMinMax(const std::pair<int, int>& minMax);
    int32_t getParentNodeId() const;
    void setParentNodeId(int32_t parentNodeId);

  private:
    int32_t id;
    std::string operatorName;
    int32_t rightChildId = -1;
    int32_t leftChildId = -1;
    std::pair<int32_t, int32_t> minMax;
    int32_t parentNodeId = -1;
};

}// namespace NES::Parsers

#endif  // NES_COORDINATOR_INCLUDE_PARSERS_NEBULAPSL_NEBULAPSLOPERATORNODE_HPP_
