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

class Node;
typedef std::shared_ptr<Node> NodePtr;

class OperatorNode;
typedef std::shared_ptr<OperatorNode> OperatorNodePtr;

class FilterLogicalOperatorNode;
typedef std::shared_ptr<FilterLogicalOperatorNode> FilterLogicalOperatorNodePtr;

class MapLogicalOperatorNode;
typedef std::shared_ptr<MapLogicalOperatorNode> MapLogicalOperatorNodePtr;

class WindowLogicalOperatorNode;
typedef std::shared_ptr<WindowLogicalOperatorNode> WindowLogicalOperatorNodePtr;

class JoinLogicalOperatorNode;
typedef std::shared_ptr<JoinLogicalOperatorNode> JoinLogicalOperatorNodePtr;

class RenameStreamOperatorNode;
typedef std::shared_ptr<RenameStreamOperatorNode> RenameStreamOperatorNodePtr;

class WatermarkAssignerLogicalOperatorNode;
typedef std::shared_ptr<WatermarkAssignerLogicalOperatorNode> WatermarkAssignerLogicalOperatorNodePtr;

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
     * @param context: the context of Z3
     * @param operatorNode: the input operator
     * @return the object representing signature created by the operator and its children
     */
    static QuerySignaturePtr createQuerySignatureForOperator(z3::ContextPtr context, OperatorNodePtr operatorNode);

  private:
    /**
     * @brief Compute a CNF representation of Conds based on signatures from children operators
     * @param context : z3 context
     * @param children : immediate children operators
     * @return Signature based on children signatures
     */
    static QuerySignaturePtr buildQuerySignatureForChildren(z3::ContextPtr context, std::vector<NodePtr> children);

    /**
     * @brief Compute query signature for Map operator
     * @param context: z3 context
     * @param mapOperator: the map operator
     * @return Signature based on window operator and its children signatures
     */
    static QuerySignaturePtr createQuerySignatureForMap(z3::ContextPtr context, MapLogicalOperatorNodePtr mapOperator);

    /**
     * @brief Compute query signature for Filter operator
     * @param context: z3 context
     * @param filterOperator: the Filter operator
     * @return Signature based on window operator and its children signatures
     */
    static QuerySignaturePtr createQuerySignatureForFilter(z3::ContextPtr context, FilterLogicalOperatorNodePtr filterOperator);

    /**
     * @brief Compute query signature for window operator
     * @param context: z3 context
     * @param windowOperator: the window operator
     * @return Signature based on window operator and its children signatures
     */
    static QuerySignaturePtr createQuerySignatureForWindow(z3::ContextPtr context, WindowLogicalOperatorNodePtr windowOperator);

    /**
     * @brief compute signature for join operator
     * @param context: z3 context
     * @param joinOperator: the join operator
     * @return Signature based on join operator and its children signatures
     */
    static QuerySignaturePtr createQuerySignatureForJoin(z3::ContextPtr context, JoinLogicalOperatorNodePtr joinOperator);

    /**
     * @brief compute signature for watermark operator
     * @param context: z3 context
     * @param watermarkOperator: the watermark operator
     * @return Signature based on watermark operator and its child signature
     */
    static QuerySignaturePtr createQuerySignatureForWatermark(z3::ContextPtr context,
                                                              WatermarkAssignerLogicalOperatorNodePtr watermarkOperator);

    /**
     * @brief Compute signature for RenameStream operator
     * @param context: z3 context
     * @param renameStreamOperator: the rename stream operator
     * @return Signature based on rename stream and its child signature
     */
    static QuerySignaturePtr createQuerySignatureForRenameStream(z3::ContextPtr context,
                                                          RenameStreamOperatorNodePtr renameStreamOperator);

    /**
     * @brief substitute the operands within the input expression with the operand's expressions value computed by upstream
     * operators and stored in column map of the child operator
     * @param context: the z3 context
     * @param inputExpr: the input expression
     * @param operandFieldMap: the operand field map containing list of operands inside the input expression
     * @param columns: the column map containing operand values used for substitution
     * @return the updated expression with substituted operands
     */
    static z3::ExprPtr substituteIntoInputExpression( z3::ContextPtr context, z3::ExprPtr inputExpr,
                                                     std::map<std::string, z3::ExprPtr>& operandFieldMap,
                                                     std::map<std::string, z3::ExprPtr>& columns);
};
}// namespace NES::Optimizer

#endif//NES_OPTIMIZE_UTILS_OPERATORTOQUERYPLANSIGNATUREUTIL_HPP
