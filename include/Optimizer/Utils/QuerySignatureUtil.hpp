/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_OPTIMIZE_UTILS_OPERATORTOQUERYPLANSIGNATUREUTIL_HPP
#define NES_OPTIMIZE_UTILS_OPERATORTOQUERYPLANSIGNATUREUTIL_HPP

#include <memory>

namespace z3 {

class expr;
typedef std::shared_ptr<expr> ExprPtr;

class context;
typedef std::shared_ptr<context> ContextPtr;
}// namespace z3

namespace NES {
class OperatorNode;
typedef std::shared_ptr<OperatorNode> OperatorNodePtr;

class FilterLogicalOperatorNode;
typedef std::shared_ptr<FilterLogicalOperatorNode> FilterLogicalOperatorNodePtr;

class MapLogicalOperatorNode;
typedef std::shared_ptr<MapLogicalOperatorNode> MapLogicalOperatorNodePtr;

class WindowLogicalOperatorNode;
typedef std::shared_ptr<WindowLogicalOperatorNode> WindowLogicalOperatorNodePtr;

class Schema;
typedef std::shared_ptr<Schema> SchemaPtr;

}// namespace NES

namespace NES::Optimizer {

class QuerySignature;
typedef std::shared_ptr<QuerySignature> QuerySignaturePtr;

/**
 * @brief This class is responsible for creating the Query Plan Signature for the input operator.
 * The query plan is composed of the input operator and all its upstream child operators.
 */
class QuerySignatureUtil {
  public:
    /**
     * @brief Convert input operator into an equivalent logical expression
     * @param operatorNode: the input operator
     * @param childrenQuerySignatures: vector containing signatures for each of the children operators (empty for source operator)
     * @param context: the context of Z3
     * @return the object representing signature created by the operator and its children
     */
    static QuerySignaturePtr createQuerySignatureForOperator(OperatorNodePtr operatorNode,
                                                             std::vector<QuerySignaturePtr> childrenQuerySignatures,
                                                             z3::ContextPtr context);

  private:
    /**
     * @brief Compute a CNF representation of Conds based on signatures from children operators
     * @param context : z3 context
     * @param childrenQuerySignatures : signatures of immediate children
     * @return Signature based on children signatures
     */
    static QuerySignaturePtr buildFromChildrenSignatures(z3::ContextPtr context,
                                                         std::vector<QuerySignaturePtr> childrenQuerySignatures);

    /**
     * @brief Compute query signature for Map operator
     * @param context: z3 context
     * @param childrenQuerySignatures: signatures of immediate children
     * @param mapOperator: the map operator
     * @return Signature based on window operator and its children signatures
     */
    static QuerySignaturePtr createQuerySignatureForMap(z3::ContextPtr context,
                                                        std::vector<QuerySignaturePtr> childrenQuerySignatures,
                                                        MapLogicalOperatorNodePtr mapOperator);

    /**
     * @brief Compute query signature for Filter operator
     * @param context: z3 context
     * @param childrenQuerySignatures: signatures of immediate children
     * @param filterOperator: the Filter operator
     * @return Signature based on window operator and its children signatures
     */
    static QuerySignaturePtr createQuerySignatureForFilter(z3::ContextPtr context,
                                                           std::vector<QuerySignaturePtr> childrenQuerySignatures,
                                                           FilterLogicalOperatorNodePtr filterOperator);

    /**
     * @brief Compute query signature for window operator
     * @param context: z3 context
     * @param childrenQuerySignatures: signatures of immediate children
     * @param windowOperator: the window operator
     * @return Signature based on window operator and its children signatures
     */
    static QuerySignaturePtr createQuerySignatureForWindow(z3::ContextPtr context,
                                                           std::vector<QuerySignaturePtr> childrenQuerySignatures,
                                                           WindowLogicalOperatorNodePtr windowOperator);

    /**
     * @brief Update the column expressions based on the output schema of the operator. This method will:
     * 1. remove the columns which are not part of the output schema
     * 2. will add the columns which are not included in the old Column map
     * 3. will copy the column expressions from the old column map
     * @param context: z3 context
     * @param outputSchema : the output schema
     * @param oldColumnMap : the old column map
     * @return map of column name to column expressions based on output schema
     */
    static std::map<std::string, std::vector<z3::ExprPtr>>
    updateQuerySignatureColumns(z3::ContextPtr context, SchemaPtr outputSchema,
                                std::map<std::string, std::vector<z3::ExprPtr>> oldColumnMap);
};
}// namespace NES::Optimizer

#endif//NES_OPTIMIZE_UTILS_OPERATORTOQUERYPLANSIGNATUREUTIL_HPP
