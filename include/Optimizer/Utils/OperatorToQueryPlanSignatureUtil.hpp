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
class context;
typedef std::shared_ptr<context> ContextPtr;
}// namespace z3

namespace NES {
class OperatorNode;
typedef std::shared_ptr<OperatorNode> OperatorNodePtr;
}// namespace NES

namespace NES::Optimizer {

class QuerySignature;
typedef std::shared_ptr<QuerySignature> QuerySignaturePtr;

/**
 * @brief This class is responsible for creating the Query Plan Signature for the input operator.
 * The query plan is composed of the input operator and all its upstream child operators.
 */
class OperatorToQueryPlanSignatureUtil {
  public:
    /**
     * @brief Convert input operator into an equivalent logical expression
     * @param operatorNode: the input operator
     * @param subQuerySignatures: vector containing signatures for each of the operator child (empty for source operator)
     * @param context: the context of Z3
     * @return the object representing signature of the query plan created by the operator and its children
     */
    static QuerySignaturePtr createForOperator(OperatorNodePtr operatorNode, std::vector<QuerySignaturePtr> subQuerySignatures,
                                               z3::ContextPtr context);

  private:
    /**
     * @brief Compute a CNF representation of Conds based on signatures from children operators
     * @param context : z3 context
     * @param subQuerySignatures : signatures of immediate children
     * @return Signature based on children signatures
     */
    static QuerySignaturePtr buildFromSubQuerySignatures(z3::ContextPtr context,
                                                         std::vector<QuerySignaturePtr> subQuerySignatures);
};
}// namespace NES::Optimizer

#endif//NES_OPTIMIZE_UTILS_OPERATORTOQUERYPLANSIGNATUREUTIL_HPP
