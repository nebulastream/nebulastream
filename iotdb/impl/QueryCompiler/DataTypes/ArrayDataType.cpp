
#include <QueryCompiler/CCodeGenerator/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statement.hpp>
#include <QueryCompiler/CodeExpression.hpp>
#include <QueryCompiler/DataTypes/ArrayDataType.hpp>
#include <QueryCompiler/DataTypes/BasicDataType.hpp>
#include <Util/Logger.hpp>

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/export.hpp>
BOOST_CLASS_EXPORT(NES::ArrayDataType);

namespace NES {

ArrayDataType::ArrayDataType(DataTypePtr ptr, u_int32_t dimension)
    : DataType(), componentDataType(ptr), dimensions(dimension) {
}

ArrayDataType::~ArrayDataType() {}

ValueTypePtr ArrayDataType::getDefaultInitValue() const { return ValueTypePtr(); }

ValueTypePtr ArrayDataType::getNullValue() const { return ValueTypePtr(); }

uint32_t ArrayDataType::getSizeBytes() const { return (dimensions * componentDataType->getSizeBytes()); }

const std::string ArrayDataType::toString() const {
    std::stringstream sstream;
    sstream << componentDataType->toString() << "[" << dimensions << "]";
    return sstream.str();
}
const std::string ArrayDataType::convertRawToString(void* data) const {
    std::stringstream str;
    // check if the pointer is valid
    if (!data)
        return "";

    char* pointer = static_cast<char*>(data);

    if (!isCharDataType())
        str << '[';
    for (u_int32_t dimension = 0; dimension < dimensions; dimension++) {
        if ((dimension != 0) && !isCharDataType())
            str << ", ";
        auto fieldOffset = componentDataType->getSizeBytes();
        auto componentValue = &pointer[fieldOffset * dimension];
        str << componentDataType->convertRawToString(componentValue);
    }
    if (!isCharDataType())
        str << ']';
    return str.str();
}

const CodeExpressionPtr ArrayDataType::getDeclCode(const std::string& identifier) const {
    CodeExpressionPtr ptr;
    if (identifier != "") {
        ptr = componentDataType->getCode();
        ptr = combine(ptr, std::make_shared<CodeExpression>(" " + identifier));
        ptr = combine(ptr, std::make_shared<CodeExpression>("["));
        ptr = combine(ptr, std::make_shared<CodeExpression>(std::to_string(dimensions)));
        ptr = combine(ptr, std::make_shared<CodeExpression>("]"));
    } else {
        ptr = componentDataType->getCode();
    }
    return ptr;
}

const StatementPtr ArrayDataType::getStmtCopyAssignment(const AssignmentStatment& aParam) const {
    FunctionCallStatement func_call("memcpy");
    func_call.addParameter(VarRef(aParam.lhs_tuple_var)[VarRef(aParam.lhs_index_var)].accessRef(VarRef(aParam.lhs_field_var)));
    func_call.addParameter(VarRef(aParam.rhs_tuple_var)[VarRef(aParam.rhs_index_var)].accessRef(VarRef(aParam.rhs_field_var)));
    func_call.addParameter(ConstantExprStatement(createBasicTypeValue(UINT64, std::to_string(this->dimensions))));
    return func_call.copy();
}

const CodeExpressionPtr ArrayDataType::getCode() const {
    std::stringstream str;
    str << "[" << dimensions << "]";
    return combine(componentDataType->getCode(), std::make_shared<CodeExpression>(str.str()));
}

const bool ArrayDataType::isEqual(DataTypePtr ptr) const { return (this->toString().compare(ptr->toString()) == 0); };
const bool ArrayDataType::isCharDataType() const { return this->componentDataType->isCharDataType(); }
const bool ArrayDataType::isArrayDataType() const { return true; }

const CodeExpressionPtr ArrayDataType::getTypeDefinitionCode() const {
    return std::make_shared<CodeExpression>("");
}

const u_int32_t ArrayDataType::getDimensions() const {
    return dimensions;
}

DataTypePtr ArrayDataType::getComponentDataType() {
    return componentDataType;
}

const DataTypePtr ArrayDataType::copy() const {
    return std::make_shared<ArrayDataType>(*this);
}

bool ArrayDataType::operator==(const DataType& _rhs) const {
    auto rhs = dynamic_cast<const NES::ArrayDataType&>(_rhs);
    return componentDataType == rhs.componentDataType && dimensions == rhs.dimensions;
}

const DataTypePtr createArrayDataType(const BasicType& type, uint32_t dimension) {
    return std::make_shared<ArrayDataType>(ArrayDataType(BasicDataType(type).copy(), dimension));
}

}// namespace NES
