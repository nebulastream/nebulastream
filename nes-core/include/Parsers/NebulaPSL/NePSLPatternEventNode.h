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

#ifndef NES_NEPSLPATTERN_H
#define NES_NEPSLPATTERN_H

#include "API/Query.hpp"
#include "NePSLPatternEventNode.h"
#include <list>
#include <queue>
#include <string>

namespace NES {

/**
 * @brief This class defines the attributes and methods used by the PatternParsingService.
 * This enables the parsing of declarative patterns into NES queries.
 */

class NePSLPatternEventNode {
  private:
    int id;
    std::string eventName;
    int rightChildId;
    int leftChildId;

  public:
    int getRightChildId() const;
    void setRightChildId(int right_child_id);
    int getLeftChildId() const;
    void setLeftChildId(int left_child_id);

  private:
    int parentNodeId = -1;
    bool negated;
    bool iteration;

    uint64_t iterMin;
    uint64_t iterMax;
    NES::Query query = NES::Query(NULL);

  public:
    //Constructors
    NePSLPatternEventNode() = default;
    explicit NePSLPatternEventNode(int id);
    // Getter and Setter
    int getId() const;
    void setId(int id);
    int getParentNodeId() const;
    void setParentNodeId(int parent);
    const std::string& getEventName() const;
    void setEventName(const std::string& eventName);

    bool isNegated() const;
    void setNegated(bool negated);
    bool isIteration() const;
    void setIteration(bool iteration);
    uint64_t getIterMin() const;
    void setIterMin(uint64_t iterMin);
    uint64_t getIterMax() const;
    void setIterMax(uint64_t iterMax);
    const NES::Query& getQuery() const;
    void setQuery(const NES::Query& query);
    void printPattern();
};

}


#endif//NES_NEPSLPATTERN_H
