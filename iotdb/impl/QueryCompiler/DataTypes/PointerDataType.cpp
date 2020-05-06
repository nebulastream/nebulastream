#include <QueryCompiler/CCodeGenerator/Statement.hpp>
#include <QueryCompiler/CodeExpression.hpp>
#include <QueryCompiler/DataTypes/PointerDataType.hpp>

namespace NES {

PointerDataType::PointerDataType(const DataTypePtr& type) : DataType(), base_type_(type) {}
ValueTypePtr PointerDataType::getDefaultInitValue() const { return ValueTypePtr(); }

ValueTypePtr PointerDataType::getNullValue() const { return ValueTypePtr(); }
uint32_t PointerDataType::getSizeBytes() const {
    /* assume a 64 bit architecture, each pointer is 8 bytes */
    return 8;
}
const std::string PointerDataType::toString() const { return base_type_->toString() + "*"; }
const std::string PointerDataType::convertRawToString(void* data) const {
    if (!data)
        return "";
    return "POINTER";// std::to_string(data);
}
const bool PointerDataType::isArrayDataType() const { return false; }

const CodeExpressionPtr PointerDataType::getDeclCode(const std::string& identifier) const {
    return std::make_shared<CodeExpression>(base_type_->getCode()->code_ + "* " + identifier);
}

const CodeExpressionPtr PointerDataType::getCode() const {
    return std::make_shared<CodeExpression>(base_type_->getCode()->code_ + "*");
}

const bool PointerDataType::isCharDataType() const { return this->base_type_->isCharDataType(); }

const bool PointerDataType::isEqual(DataTypePtr ptr) const {
    std::shared_ptr<PointerDataType> temp = std::dynamic_pointer_cast<PointerDataType>(ptr);
    if (temp)
        return isEqual(temp);
    return false;
}

const bool PointerDataType::isEqual(std::shared_ptr<PointerDataType> btr) const {
    return base_type_->isEqual(btr->base_type_);
}

const CodeExpressionPtr PointerDataType::getTypeDefinitionCode() const { return base_type_->getTypeDefinitionCode(); }

const DataTypePtr PointerDataType::copy() const {
    return std::make_shared<PointerDataType>(*this);
}

bool PointerDataType::operator==(const DataType& _rhs) const {
    auto rhs = dynamic_cast<const NES::PointerDataType&>(_rhs);
    return base_type_ == rhs.base_type_;
}

const DataTypePtr createPointerDataType(const DataTypePtr& type) { return std::make_shared<PointerDataType>(type); }

const DataTypePtr createPointerDataType(const BasicType& type) {
    return std::make_shared<PointerDataType>(createDataType(type));
}
}// namespace NES