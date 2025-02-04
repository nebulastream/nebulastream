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

#ifndef NES_COORDINATOR_INCLUDE_PARSERS_NEBULAPSL_NEBULAPSLPATTERN_HPP_
#define NES_COORDINATOR_INCLUDE_PARSERS_NEBULAPSL_NEBULAPSLPATTERN_HPP_

#include <API/QueryAPI.hpp>
#include <Parsers/NebulaPSL/NebulaPSLOperator.hpp>
#include <list>
#include <map>
#include <string>

namespace NES::Parsers {

// Define the Parameter structure
struct WindowParameter {
    std::string name;                 // Name of the parameter
    std::pair<std::string, int> value;// The old pair (string and int)
};

// Define the Window structure
struct Window {
    std::string type;                       // Type of the window
    std::string timeAttributeName;          // Name of the time attribute
    std::vector<WindowParameter> parameters;// List of parameters
};

/**
 * @brief This class represents the results from parsing the ANTLR AST tree
 * Attributes of this class represent the different clauses and a merge into a query after parsing the AST
 */
class NebulaPSLPattern {
  public:
    //Constructors
    NebulaPSLPattern() = default;
    // Getter and Setter
    const std::map<int32_t, std::string>& getSources() const;
    const std::map<std::string, std::string>& getAliasList() const;
    const std::map<int32_t, NebulaPSLOperator>& getOperatorList() const;
    const std::list<ExpressionNodePtr>& getExpressions() const;
    const std::vector<ExpressionNodePtr>& getProjectionFields() const;
    const std::list<SinkDescriptorPtr>& getSinks() const;
    Window getWindow() const;
    void setWindow(Window window);

    /**
     * @brief inserts a new source into the source map (FROM-Clause)
     * @param sourcePair <K,V> containing the nodeId and the streamName
     */
    void addSource(std::pair<int32_t, std::basic_string<char>> sourcePair);

    /**
     * @brief updates the sourceName in case an alias is using in the FROM-Clause
     * @param sourcePair <K,V> containing the nodeId and the streamName
     */
    void updateSource(const int32_t key, std::string sourceName);

    void updateAliasList(std::string aliasName, std::string sourceName);
    /**
     * @brief inserts a new ExpressionNode (filter) to the list of parsed expressions (WHERE-clause)
     * @param expNode
     */
    void addExpression(ExpressionNodePtr expressionNode);

    /**
     * @brief inserts a new Sink to the list of parsed sinks
     * @param sink
     */
    void addSink(SinkDescriptorPtr sinkDescriptor);

    /**
     * @brief inserts a new ExpressionItem (Projection Attribute) to the  list of specified output attributes
     * @param expressionItem
     */
    void addProjectionField(ExpressionNodePtr expressionNode);

    /**
     * @brief inserts a new Operator from the PATTERN clause to the list of operators
     * @param opNode
     */
    void addOperator(NebulaPSLOperator operatorNode);

  private:
    std::map<int32_t, std::string> sourceList;        // the list of source in input order
    std::map<std::string, std::string> aliasList;     // the list of sources aliases used in the pattern
    std::map<int32_t, NebulaPSLOperator> operatorList;// contains the operators from the PATTERN clause
    std::list<ExpressionNodePtr> expressionList;
    std::vector<ExpressionNodePtr> projectionFields;
    std::list<SinkDescriptorPtr> sinkList;// INTO
    Window window;                        // WITHIN
};

}// namespace NES::Parsers

#endif// NES_COORDINATOR_INCLUDE_PARSERS_NEBULAPSL_NEBULAPSLPATTERN_HPP_
