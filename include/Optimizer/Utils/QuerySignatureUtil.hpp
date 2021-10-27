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

#ifndef NES_INCLUDE_OPTIMIZER_UTILS_QUERY_SIGNATURE_UTIL_HPP_
#define NES_INCLUDE_OPTIMIZER_UTILS_QUERY_SIGNATURE_UTIL_HPP_

#include <Operators/OperatorForwardDeclaration.hpp>
#include <memory>

namespace z3 {

class expr;
using ExprPtr = std::shared_ptr<expr>;

class context;
using ContextPtr = std::shared_ptr<context>;
}// namespace z3

namespace NES {

class Node;
using NodePtr = std::shared_ptr<Node>;

}// namespace NES

namespace NES::Optimizer {

class QuerySignature;
using QuerySignaturePtr = std::shared_ptr<QuerySignature>;

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
    static QuerySignaturePtr createQuerySignatureForOperator(const z3::ContextPtr& context, const OperatorNodePtr& operatorNode);

  private:
    /**
     * @brief Compute a query signature for Source operator
     * @param context: z3 context
     * @param sourceOperator: the source operator
     * @return Signature based on source operator
     */
    static QuerySignaturePtr createQuerySignatureForSource(const z3::ContextPtr& context,
                                                           const SourceLogicalOperatorNodePtr& sourceOperator);

    /**
     * @brief Compute a query signature for Project operator
     * @param projectOperator: the project operator
     * @return Signature based on project operator and its children signatures
     */
    static QuerySignaturePtr createQuerySignatureForProject(const ProjectionLogicalOperatorNodePtr& projectOperator);

    /**
     * @brief Compute a query signature for Map operator
     * @param context: z3 context
     * @param mapOperator: the map operator
     * @return Signature based on window operator and its children signatures
     */
    static QuerySignaturePtr createQuerySignatureForMap(const z3::ContextPtr& context,
                                                        const MapLogicalOperatorNodePtr& mapOperator);

    /**
     * @brief Compute a query signature for Filter operator
     * @param context: z3 context
     * @param filterOperator: the Filter operator
     * @return Signature based on filter operator and its children signatures
     */
    static QuerySignaturePtr createQuerySignatureForFilter(const z3::ContextPtr& context,
                                                           const FilterLogicalOperatorNodePtr& filterOperator);

    /**
     * @brief Compute a query signature for window operator
     * @param context: z3 context
     * @param windowOperator: the window operator
     * @return Signature based on window operator and its children signatures
     */
    static QuerySignaturePtr createQuerySignatureForWindow(const z3::ContextPtr& context,
                                                           const WindowLogicalOperatorNodePtr& windowOperator);

    /**
     * @brief compute a signature for join operator
     * @param context: z3 context
     * @param joinOperator: the join operator
     * @return Signature based on join operator and its children signatures
     */
    static QuerySignaturePtr createQuerySignatureForJoin(const z3::ContextPtr& context,
                                                         const JoinLogicalOperatorNodePtr& joinOperator);

    /**
     * @brief compute a signature for watermark operator
     * @param context: z3 context
     * @param watermarkAssignerOperator: the watermark operator
     * @return Signature based on watermark operator and its child signature
     */
    static QuerySignaturePtr
    createQuerySignatureForWatermark(const z3::ContextPtr& context,
                                     const WatermarkAssignerLogicalOperatorNodePtr& watermarkAssignerOperator);

    /**
     * @brief Compute a signature for Union operator
     * @param context: z3 context
     * @param unionOperator: the union operator
     * @return Signature based on union and its child signatures
     */
    static QuerySignaturePtr createQuerySignatureForUnion(const z3::ContextPtr& context,
                                                          const UnionLogicalOperatorNodePtr& unionOperator);
};
}// namespace NES::Optimizer

#endif  // NES_INCLUDE_OPTIMIZER_UTILS_QUERY_SIGNATURE_UTIL_HPP_
