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

#ifndef NES_PARSERS_NEBULAPSL_NEBULAPSLOPERATORNODE_H_
#define NES_PARSERS_NEBULAPSL_NEBULAPSLOPERATORNODE_H_

#include "API/Query.hpp"
#include <list>
#include <queue>
#include <string>

namespace NES {

/**
 * @brief This class defines the attributes and methods used by the PatternParsingService.
 * This enables the parsing of declarative patterns into NES queries.
 */

class NebulaPSLOperatorNode {
  private:
    int id;
    std::string eventName;
    int rightChildId = -1;
    int leftChildId = -1;
    std::pair<int, int> minMax;
    int parentNodeId = -1;
    NES::Query query = NES::Query(NULL);

  public:
    //Constructors
    explicit NebulaPSLOperatorNode(int id);
    // Getter and Setter
    int getId() const;
    void setId(int id);
    const std::string& getEventName() const;
    void setEventName(const std::string& event_name);
    int getRightChildId() const;
    void setRightChildId(int right_child_id);
    int getLeftChildId() const;
    void setLeftChildId(int left_child_id);
    const std::pair<int, int>& getMinMax() const;
    void setMinMax(const std::pair<int, int>& min_max);
    int getParentNodeId() const;
    void setParentNodeId(int parent_node_id);
    const Query& getQuery() const;
    void setQuery(const Query& query);
};

}// namespace NES

#endif//NES_PARSERS_NEBULAPSL_NEBULAPSLOPERATORNODE_H_
