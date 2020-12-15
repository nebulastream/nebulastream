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


#include <Optimizer/QueryValidation/SemanticQueryValidation.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/OperatorNode.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger.hpp>
#include <string.h>
#include <z3++.h>

namespace NES {

bool NES::SemanticQueryValidation::isSatisfiable(QueryPtr inputQuery) {

    SchemaPtr schema;
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    auto sourceOperators = inputQuery->getQueryPlan()->getSourceOperators();

    for (auto source : sourceOperators) {
        auto sourceDescriptor = source->getSourceDescriptor();

        // if the source descriptor is only a logical stream source we have to replace it with the correct
        // source descriptor form the catalog.
        if (sourceDescriptor->instanceOf<LogicalStreamSourceDescriptor>()) {
            auto streamName = sourceDescriptor->getStreamName();
            
            try {
                schema = streamCatalog->getSchemaForLogicalStream(streamName);
            } catch (std::exception& e) {
                NES_THROW_RUNTIME_ERROR("SemanticQueryValidation: The logical stream " + streamName
                                        + " could not be found in the StreamCatalog");
            }
        }
    }
    
    std::shared_ptr<z3::context> c = std::make_shared<z3::context>();
    z3::solver s(*c);

    auto filterOperators = inputQuery->getQueryPlan()->getOperatorByType<FilterLogicalOperatorNode>();

    for (int i = 0; i < filterOperators.size(); i++) {
        filterOperators[i]->getPredicate()->inferStamp(schema);
    }
    for (int i = 0; i < filterOperators.size(); i++) {
        filterOperators[i]->inferZ3Expression(c);
    }
    for (int i = 0; i < filterOperators.size(); i++) {
        s.add(filterOperators[i]->getZ3Expression());
    }

    return s.check() != z3::unsat;
}

}// namespace NES