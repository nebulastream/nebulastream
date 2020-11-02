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