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

#pragma once

#include <memory>
#include <Operators/LogicalOperators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceNameLogicalOperator.hpp>

namespace NES
{

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

class Node;
using NodePtr = std::shared_ptr<Node>;

class SinkLogicalOperator;
using SinkLogicalOperatorPtr = std::shared_ptr<SinkLogicalOperator>;

class DecomposedQueryPlan;
using DecomposedQueryPlanPtr = std::shared_ptr<DecomposedQueryPlan>;

namespace Catalogs
{

namespace Source
{
class SourceCatalog;
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;
}

namespace UDF
{
class UDFCatalog;
using UDFCatalogPtr = std::shared_ptr<UDFCatalog>;
}

}

namespace Optimizer
{

class TypeInferencePhase;
using TypeInferencePhasePtr = std::shared_ptr<TypeInferencePhase>;

/// The type inference phase receives and query plan and infers all input and output schemata for all operators.
/// If this is not possible it throws an Runtime exception.
class TypeInferencePhase
{
public:
    static TypeInferencePhasePtr create(Catalogs::Source::SourceCatalogPtr sourceCatalog);

    /// For each source, sets the schema by getting it from the source catalog and formatting the field names (adding a prefix qualifier name).
    /// @throws LogicalSourceNotFoundInQueryDescription if inferring the data types into the query failed
    void performTypeInferenceSources(const std::vector<std::shared_ptr<SourceNameLogicalOperator>>& sourceOperators, QueryId queryId) const;

    /// Performs type inference on the given query plan.
    /// This involves the following steps.
    /// 1. Replacing a logical source descriptor with the correct source descriptor form the source catalog.
    /// 2. Propagate the input and output schemas from source operators to the sink operators.
    /// 3. If a operator contains expression, we infer the result stamp of this operators.
    /// @throws TypeInferenceException if inferring the data types into the query failed
    QueryPlanPtr performTypeInferenceQuery(QueryPlanPtr queryPlan);

private:
    Catalogs::Source::SourceCatalogPtr sourceCatalog;

    explicit TypeInferencePhase(Catalogs::Source::SourceCatalogPtr sourceCatalog);
};
}
}
