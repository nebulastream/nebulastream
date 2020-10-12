#include <QueryCompiler/GeneratedCode.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>

namespace NES {

WindowAggregationDescriptor::WindowAggregationDescriptor(const NES::AttributeFieldPtr onField) : onField(onField),
                                                                             asField(const_cast<AttributeFieldPtr&>(onField)) {
}

WindowAggregationDescriptor::WindowAggregationDescriptor(const NES::AttributeFieldPtr onField, const NES::AttributeFieldPtr asField) : onField(onField), asField(asField) {
}

WindowAggregationDescriptor& WindowAggregationDescriptor::as(const NES::AttributeFieldPtr asField) {
    this->asField = asField;
    return *this;
}

AttributeFieldPtr WindowAggregationDescriptor::as() {
    if (asField == nullptr) {
        return onField;
    }
    return asField;
}

AttributeFieldPtr WindowAggregationDescriptor::on() {
    return onField;
}
}// namespace NES