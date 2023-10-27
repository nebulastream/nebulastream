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

#include <Parsers/NebulaSQL/NebulaSQLOperatorNode.hpp>
#include <Parsers/NebulaSQL/NebulaSQLHelper.hpp>

namespace NES::Parsers {

        const std::vector<std::string>& NebulaSQLHelper::getSources() const { return this->sources; }
        void NebulaSQLHelper::addSource(const std::string& source) {
            this->sources.push_back(source);
        }

        const SinkDescriptorPtr NebulaSQLHelper::getSinkDescriptor() const {
            return this->sinkDescriptor;
        }
        void NebulaSQLHelper::setSink(SinkDescriptorPtr sink) {
            this->sinkDescriptor = sink;
        }

        const std::vector<ExpressionNodePtr>& NebulaSQLHelper::getWhereClauses() const {
            return whereClauses;
        }
        void NebulaSQLHelper::addWhereClause(ExpressionNodePtr expression) {
            whereClauses.push_back(expression);
        }
        void NebulaSQLHelper::setProjectionFields(const std::vector<ExpressionNodePtr>& projectionFields) {
            this->projectionFields = projectionFields;
        }
        void NebulaSQLHelper::addProjectionField(ExpressionNodePtr expressionNode) { this->projectionFields.push_back(expressionNode); }
        const std::vector<ExpressionNodePtr>& NebulaSQLHelper::getProjectionFields() const { return this->projectionFields; }
        uint64_t NebulaSQLHelper::getLimit() const { return 0; }
        const std::string& NebulaSQLHelper::getNewName() const { return <#initializer #>; }
        const FieldAssignmentExpressionNodePtr& NebulaSQLHelper::getMapExpression() const { return <#initializer #>; }
        const WatermarkStrategyDescriptorPtr& NebulaSQLHelper::getWatermarkStrategieDescriptor() const {
            return <#initializer #>;
        }
        const NES::Windowing::WindowTypePtr NebulaSQLHelper::getWindowType() const { return NES::Windowing::WindowTypePtr(); }

    }// namespace NES::Parsers
