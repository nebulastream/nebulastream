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

#include <Optimizer/Utils/OperatorTupleSchemaMap.hpp>

namespace NES::Optimizer {

OperatorTupleSchemaMap::OperatorTupleSchemaMap() : schemaMaps() {}

OperatorTupleSchemaMap::OperatorTupleSchemaMap(std::vector<std::map<std::string, z3::ExprPtr>> schemaMaps)
    : schemaMaps(schemaMaps) {}

OperatorTupleSchemaMapPtr OperatorTupleSchemaMap::create(std::vector<std::map<std::string, z3::ExprPtr>> schemaMaps) {
    return std::make_shared<OperatorTupleSchemaMap>(OperatorTupleSchemaMap(schemaMaps));
}

OperatorTupleSchemaMapPtr OperatorTupleSchemaMap::createEmpty() {
    return std::make_shared<OperatorTupleSchemaMap>(OperatorTupleSchemaMap());
}

const std::vector<std::map<std::string, z3::ExprPtr>>& OperatorTupleSchemaMap::getSchemaMaps() const { return schemaMaps; }
}// namespace NES::Optimizer
