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

#ifndef NES_INCLUDE_OPERATORS_LOGICALOPERATORS_OPERATORFORWARDDECLARATION_HPP_
#define NES_INCLUDE_OPERATORS_LOGICALOPERATORS_OPERATORFORWARDDECLARATION_HPP_
#include <memory>
namespace NES {

class Schema;
typedef std::shared_ptr<Schema> SchemaPtr;

class OperatorNode;
typedef std::shared_ptr<OperatorNode> OperatorNodePtr;

class ExpressionNode;
typedef std::shared_ptr<ExpressionNode> ExpressionNodePtr;

class FilterLogicalOperatorNode;
typedef std::shared_ptr<FilterLogicalOperatorNode> FilterLogicalOperatorNodePtr;

class JoinLogicalOperatorNode;
typedef std::shared_ptr<JoinLogicalOperatorNode> JoinLogicalOperatorNodePtr;

class MergeLogicalOperatorNode;
typedef std::shared_ptr<MergeLogicalOperatorNode> MergeLogicalOperatorNodePtr;

class ExpressionItem;

class ProjectionLogicalOperatorNode;
typedef std::shared_ptr<ProjectionLogicalOperatorNode> LogicalProjectionOperatorPtr;

class MapLogicalOperatorNode;
typedef std::shared_ptr<MapLogicalOperatorNode> MapLogicalOperatorNodePtr;

}
#endif//NES_INCLUDE_OPERATORS_LOGICALOPERATORS_OPERATORFORWARDDECLARATION_HPP_
