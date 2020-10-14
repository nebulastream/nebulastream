#include <QueryCompiler/GeneratedCode.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>

namespace NES {

WindowAggregationDescriptor::WindowAggregationDescriptor(const FieldAccessExpressionNodePtr onField) : onField(onField),
                                                                             asField(onField) {
}

WindowAggregationDescriptor::WindowAggregationDescriptor(const FieldAccessExpressionNodePtr onField, const FieldAccessExpressionNodePtr asField) : onField(onField), asField(asField) {
}

WindowAggregationDescriptor& WindowAggregationDescriptor::as(const FieldAccessExpressionNodePtr asField) {
    this->asField = asField;
    return *this;
}

FieldAccessExpressionNodePtr WindowAggregationDescriptor::as() {
    if (asField == nullptr) {
        return onField;
    }
    return asField;
}

FieldAccessExpressionNodePtr WindowAggregationDescriptor::on() {
    return onField;
}
}// namespace NES