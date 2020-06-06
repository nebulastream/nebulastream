#include <QueryCompiler/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CodeExpression.hpp>
#include <QueryCompiler/DataTypes/ReferenceDataType.hpp>

namespace NES {

ReferenceDataType::ReferenceDataType(const DataTypePtr& type) : DataType(), base_type_(type) {}
ValueTypePtr ReferenceDataType::getDefaultInitValue() const { return ValueTypePtr(); }

ValueTypePtr ReferenceDataType::getNullValue() const { return ValueTypePtr(); }
uint32_t ReferenceDataType::getSizeBytes() const {
    /* assume a 64 bit architecture, each pointer is 8 bytes */
    return 8;
}
const std::string ReferenceDataType::toString() const { return base_type_->toString() + "*"; }
const std::string ReferenceDataType::convertRawToString(void* data) const {
    if (!data)
        return "";
    return "REFERENCE";// std::to_string(data);
}

bool ReferenceDataType::isArrayDataType() const { return false; }

const CodeExpressionPtr ReferenceDataType::getDeclCode(const std::string& identifier) const {
    return std::make_shared<CodeExpression>(base_type_->getCode()->code_ + "& " + identifier);
}

const CodeExpressionPtr ReferenceDataType::getCode() const {
    return std::make_shared<CodeExpression>(base_type_->getCode()->code_ + "&");
}

bool ReferenceDataType::isCharDataType() const { return this->base_type_->isCharDataType(); }

bool ReferenceDataType::isEqual(DataTypePtr ptr) const {
    std::shared_ptr<ReferenceDataType> temp = std::dynamic_pointer_cast<ReferenceDataType>(ptr);
    if (temp)
        return isEqual(temp);
    return false;
}

const bool ReferenceDataType::isEqual(std::shared_ptr<ReferenceDataType> btr) const {
    return base_type_->isEqual(btr->base_type_);
}

const CodeExpressionPtr ReferenceDataType::getTypeDefinitionCode() const { return base_type_->getTypeDefinitionCode(); }

const DataTypePtr ReferenceDataType::copy() const {
    return std::make_shared<ReferenceDataType>(*this);
}

bool ReferenceDataType::operator==(const DataType& _rhs) const {
    auto rhs = dynamic_cast<const NES::ReferenceDataType&>(_rhs);
    return base_type_ == rhs.base_type_;
}

const DataTypePtr createReferenceDataType(const DataTypePtr& type) { return std::make_shared<ReferenceDataType>(type); }

const DataTypePtr createReferenceDataType(const BasicType& type) {
    return std::make_shared<ReferenceDataType>(createDataType(type));
}
}// namespace NES