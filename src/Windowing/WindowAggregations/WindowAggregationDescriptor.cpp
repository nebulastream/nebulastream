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

#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>

namespace NES::Windowing {

WindowAggregationDescriptor::WindowAggregationDescriptor(const FieldAccessExpressionNodePtr onField) : onField(onField),
                                                                                                       asField(onField) {
}

WindowAggregationDescriptor::WindowAggregationDescriptor(const ExpressionNodePtr onField, const ExpressionNodePtr asField) : onField(onField), asField(asField) {
}

WindowAggregationDescriptor& WindowAggregationDescriptor::as(const FieldAccessExpressionNodePtr asField) {
    this->asField = asField;
    return *this;
}

ExpressionNodePtr WindowAggregationDescriptor::as() {
    if (asField == nullptr) {
        return onField;
    }
    return asField;
}

ExpressionNodePtr WindowAggregationDescriptor::on() {
    return onField;
}
}// namespace NES::Windowing