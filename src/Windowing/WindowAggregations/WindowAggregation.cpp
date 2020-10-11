#include <QueryCompiler/GeneratedCode.hpp>
#include <Windowing/WindowAggregations/WindowAggregation.hpp>

namespace NES {

WindowAggregation::WindowAggregation(const NES::AttributeFieldPtr onField) : onField(onField),
                                                                             asField(const_cast<AttributeFieldPtr&>(onField)) {
}

WindowAggregation::WindowAggregation(const NES::AttributeFieldPtr onField, const NES::AttributeFieldPtr asField) : onField(onField), asField(asField) {
}

WindowAggregation& WindowAggregation::as(const NES::AttributeFieldPtr asField) {
    this->asField = asField;
    return *this;
}

AttributeFieldPtr WindowAggregation::as() {
    if (asField == nullptr){
        return onField;  }
    return asField;
}


AttributeFieldPtr WindowAggregation::on() {
    return onField;
}
}// namespace NES