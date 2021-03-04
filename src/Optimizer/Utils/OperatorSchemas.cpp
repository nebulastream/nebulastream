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

#include <Optimizer/Utils/OperatorSchemas.hpp>

namespace NES::Optimizer {

OperatorSchemas::OperatorSchemas(std::vector<std::map<std::string, z3::ExprPtr>> &&schemaFieldNameToZ3ExpressionMaps)
    : schemaFieldNameToZ3ExpressionMaps(std::move(schemaFieldNameToZ3ExpressionMaps)) {}

OperatorSchemasPtr OperatorSchemas::create(std::vector<std::map<std::string, z3::ExprPtr>> &&schemaFieldNameToZ3ExpressionMaps) {
    return std::make_shared<OperatorSchemas>(OperatorSchemas(std::move(schemaFieldNameToZ3ExpressionMaps)));
}

const std::vector<std::map<std::string, z3::ExprPtr>>& OperatorSchemas::getOperatorSchemas() const {
    return schemaFieldNameToZ3ExpressionMaps;
}
}// namespace NES::Optimizer
