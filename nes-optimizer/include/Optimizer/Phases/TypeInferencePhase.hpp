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

#ifndef NES_OPTIMIZER_INCLUDE_OPTIMIZER_PHASES_TYPEINFERENCEPHASE_HPP_
#define NES_OPTIMIZER_INCLUDE_OPTIMIZER_PHASES_TYPEINFERENCEPHASE_HPP_

#include <Util/FaultToleranceType.hpp>
#include <memory>
#include <Util/FaultToleranceType.hpp>

namespace NES {

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

class Node;
using NodePtr = std::shared_ptr<Node>;

class SourceDescriptor;
using SourceDescriptorPtr = std::shared_ptr<SourceDescriptor>;

class SourceLogicalOperator;
using SourceLogicalOperatorPtr = std::shared_ptr<SourceLogicalOperator>;

class SinkLogicalOperator;
using SinkLogicalOperatorPtr = std::shared_ptr<SinkLogicalOperator>;

class DecomposedQueryPlan;
using DecomposedQueryPlanPtr = std::shared_ptr<DecomposedQueryPlan>;

namespace Catalogs {

namespace Source {
class SourceCatalog;
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;
}// namespace Source

namespace UDF {
class UDFCatalog;
using UDFCatalogPtr = std::shared_ptr<UDFCatalog>;
}// namespace UDF

}// namespace Catalogs

namespace Optimizer {

class TypeInferencePhase;
using TypeInferencePhasePtr = std::shared_ptr<TypeInferencePhase>;

/**
 * @brief The type inference phase receives and query plan and infers all input and output schemata for all operators.
 * If this is not possible it throws an Runtime exception.
 */
class TypeInferencePhase {
  public:
    /**
     * @brief Factory method to create a type inference phase.
     * @return TypeInferencePhasePtr
     */
    static TypeInferencePhasePtr create(Catalogs::Source::SourceCatalogPtr sourceCatalog,
                                        Catalogs::UDF::UDFCatalogPtr udfCatalog);

    /**
     * @brief Performs type inference on the given query plan.
     * This involves the following steps.
     * 1. Replacing a logical source descriptor with the correct source descriptor form the source catalog.
     * 2. Propagate the input and output schemas from source operators to the sink operators.
     * 3. If a operator contains expression, we infer the result stamp of this operators.
     * @param queryPlan the query plan
     * @param faultToleranceType fault tolerance type
     */
    QueryPlanPtr execute(QueryPlanPtr queryPlan, FaultToleranceType faultToleranceType = FaultToleranceType::NONE);

    /**
     * @brief Performs type inference on the given decomposed query plan.
     * This involves the following steps.
     * 1. Replacing a logical source descriptor with the correct source descriptor form the source catalog.
     * 2. Propagate the input and output schemas from source operators to the sink operators.
     * 3. If a operator contains expression, we infer the result stamp of this operators.
     * @param decomposedQueryPlan the decomposed query plan
     * @param faultToleranceType fault tolerance type
     */
    DecomposedQueryPlanPtr execute(DecomposedQueryPlanPtr decomposedQueryPlan,
                                   FaultToleranceType faultToleranceType = FaultToleranceType::NONE);

  private:
    /**
     * @brief Infer schema for all operators between given source and sink operators.
     * @param planId: the id of the plan
     * @param sourceOperators : the source operators
     * @param sinkOperators : the sink operators
     * @param faultToleranceType fault tolerance type
     * @throws RuntimeException if it was not possible to infer the data types of schemas and expression
     * @return QueryPlanPtr
     * @throws TypeInferenceException if inferring the data types into the query failed
     * @throws LogicalSourceNotFoundException if a logical source with the given source name could not be found
     */
    void performTypeInference(QueryId planId,
                              std::vector<SourceLogicalOperatorPtr> sourceOperators,
                              std::vector<SinkLogicalOperatorPtr> sinkOperators,
                              FaultToleranceType faultToleranceType);

    explicit TypeInferencePhase(Catalogs::Source::SourceCatalogPtr sourceCatalog, Catalogs::UDF::UDFCatalogPtr udfCatalog);
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    Catalogs::UDF::UDFCatalogPtr udfCatalog;
};
}// namespace Optimizer
}// namespace NES

#endif// NES_OPTIMIZER_INCLUDE_OPTIMIZER_PHASES_TYPEINFERENCEPHASE_HPP_
