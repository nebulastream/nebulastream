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

#ifndef NES_CORE_INCLUDE_PARSERS_NEBULASQL_NEBULASQLHELPER_HPP_
#define NES_CORE_INCLUDE_PARSERS_NEBULASQL_NEBULASQLHELPER_HPP_

#include <API/QueryAPI.hpp>
#include <Parsers/NebulaSQL/NebulaSQLOperatorNode.hpp>
#include <list>
#include <map>
#include <string>

namespace NES::Parsers {

/**
 * @brief This class represents the results from parsing the ANTLR AST tree
 * Attributes of this class represent the different clauses and a merge into a query after parsing the AST
 */
class NebulaSQLHelper {
  public:
    //Constructors
    NebulaSQLHelper() = default;
    // Getter and Setter
    const std::map<int32_t, std::string>& getSources() const;
    void setSources(const std::map<int32_t, std::string>& sources);
    const std::map<int32_t, NebulaSQLOperatorNode>& getOperatorList() const;
    void setOperatorList(const std::map<int32_t, NebulaSQLOperatorNode>& operatorList);
    const std::list<ExpressionNodePtr>& getExpressions() const;
    void setExpressions(const std::list<ExpressionNodePtr>& expressions);
    const std::vector<ExpressionNodePtr>& getProjectionFields() const;
    void setProjectionFields(const std::vector<ExpressionNodePtr>& projectionFields);
    const std::list<SinkDescriptorPtr>& getSinks() const;
    void setSinks(const std::list<SinkDescriptorPtr>& sinks);
    const std::pair<std::string, int>& getWindow() const;
    void setWindow(const std::pair<std::string, int>& window);

    /**
     * @brief inserts a new source into the source map (FROM-Clause)
     * @param sourcePair <K,V> containing the nodeId and the streamName
     */
    void addSource(std::pair<int32_t, std::basic_string<char>> sourcePair);
    /**
     * @brief inserts a new ExpressionItem (Projection Attribute) to the  list of specified output attributes
     * @param expressionItem
     */
    void addProjectionField(ExpressionNodePtr expressionNode);

    void addSink(SinkDescriptorPtr sinkDescriptor);


  private:
    std::map<int32_t, std::string> sourceList;
    std::map<int32_t, NebulaSQLOperatorNode> operatorList;// contains the operators from the HELPER clause
    std::list<ExpressionNodePtr> expressionList;
    std::vector<ExpressionNodePtr> projectionFields;
    std::list<SinkDescriptorPtr> sinkList;
    std::pair<std::string, int32_t> window;
};

}// namespace NES::Parsers

#endif// NES_CORE_INCLUDE_PARSERS_NEBULASQL_NEBULASQLHELPER_HPP_
