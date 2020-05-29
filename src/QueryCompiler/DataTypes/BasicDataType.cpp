#include <QueryCompiler/CCodeGenerator/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Statement.hpp>
#include <QueryCompiler/CodeExpression.hpp>
#include <QueryCompiler/DataTypes/BasicDataType.hpp>

#include <boost/serialization/access.hpp>
#include <boost/serialization/export.hpp>

namespace NES {

BasicDataType::BasicDataType(const BasicType& type) : type(type), dataSize(0) {}

BasicDataType::BasicDataType(const BasicType& type, uint32_t size) : type(type), dataSize(size) {}

ValueTypePtr BasicDataType::getDefaultInitValue() const { return ValueTypePtr(); }

ValueTypePtr BasicDataType::getNullValue() const { return ValueTypePtr(); }

bool BasicDataType::isEqual(DataTypePtr ptr) const {
    std::shared_ptr<BasicDataType> temp = std::dynamic_pointer_cast<BasicDataType>(ptr);
    if (temp)
        return isEqual(temp);
    return false;
}

const bool BasicDataType::isEqual(std::shared_ptr<BasicDataType> btr) const {
    return ((btr->type == this->type) && (btr->getSizeBytes() == this->getSizeBytes()));
}

bool BasicDataType::isArrayDataType() const { return false; }

bool BasicDataType::isCharDataType() const { return (this->type == BasicType::CHAR); }

uint32_t BasicDataType::getSizeBytes() const {
    if (dataSize == 0) {
        return getFixSizeBytes();
    } else {
        return getFixSizeBytes() * dataSize;
    }
}

uint32_t BasicDataType::getFixSizeBytes() const {
    switch (type) {
        case INT8: return sizeof(int8_t);
        case UINT8: return sizeof(uint8_t);
        case INT16: return sizeof(int16_t);
        case UINT16: return sizeof(uint16_t);
        case INT32: return sizeof(int32_t);
        case UINT32: return sizeof(uint32_t);
        case INT64: return sizeof(int64_t);
        case UINT64: return sizeof(uint64_t);
        case FLOAT32: return sizeof(float);
        case FLOAT64: return sizeof(double);
        case BOOLEAN: return sizeof(bool);
        case CHAR: return sizeof(char);
        case DATE: return sizeof(uint32_t);
        case VOID_TYPE: return 0;
    }
    return 0;
}
const std::string BasicDataType::toString() const {
    switch (type) {
        case INT8: return "INT8";
        case UINT8: return "UINT8";
        case INT16: return "INT16";
        case UINT16: return "UINT16";
        case INT32: return "INT32";
        case UINT32: return "UINT32";
        case INT64: return "INT64";
        case UINT64: return "UINT64";
        case FLOAT32: return "FLOAT32";
        case FLOAT64: return "FLOAT64";
        case BOOLEAN: return "BOOLEAN";
        case CHAR: return "CHAR";
        case DATE: return "DATE";
        case VOID_TYPE: return "VOID";
        case UNDEFINED: return "UNDEFINED";
    }
    return "";
}

const std::string BasicDataType::convertRawToString(void* data) const {
    if (!data)
        return std::string();
    std::stringstream str;
    switch (type) {
        case INT8: return std::to_string(*reinterpret_cast<int8_t*>(data));
        case UINT8: return std::to_string(*reinterpret_cast<uint8_t*>(data));
        case INT16: return std::to_string(*reinterpret_cast<int16_t*>(data));
        case UINT16: return std::to_string(*reinterpret_cast<uint16_t*>(data));
        case INT32: return std::to_string(*reinterpret_cast<int32_t*>(data));
        case UINT32: return std::to_string(*reinterpret_cast<uint32_t*>(data));
        case INT64: return std::to_string(*reinterpret_cast<int64_t*>(data));
        case UINT64: return std::to_string(*reinterpret_cast<uint64_t*>(data));
        case FLOAT32: return std::to_string(*reinterpret_cast<float*>(data));
        case FLOAT64: return std::to_string(*reinterpret_cast<double*>(data));
        case BOOLEAN: return std::to_string(*reinterpret_cast<bool*>(data));
        case CHAR:
            if (dataSize == 0) {
                str << static_cast<char>(*static_cast<char*>(data));
            } else {
                str << static_cast<char*>(data);
            }
            return str.str();
        case DATE:
            str << *static_cast<char*>(data);
            return str.str();
        case VOID_TYPE: return "";
    }
    return "";
}

const CodeExpressionPtr BasicDataType::getDeclCode(const std::string& identifier) const {
    std::stringstream str;
    str << " " << identifier;
    return combine(getCode(), std::make_shared<CodeExpression>(str.str()));
}

const CodeExpressionPtr BasicDataType::getCode() const {
    switch (type) {
        case INT8: return std::make_shared<CodeExpression>("int8_t");
        case UINT8: return std::make_shared<CodeExpression>("uint8_t");
        case INT16: return std::make_shared<CodeExpression>("int16_t");
        case UINT16: return std::make_shared<CodeExpression>("uint16_t");
        case INT32: return std::make_shared<CodeExpression>("int32_t");
        case UINT32: return std::make_shared<CodeExpression>("uint32_t");
        case INT64: return std::make_shared<CodeExpression>("int64_t");
        case UINT64: return std::make_shared<CodeExpression>("uint64_t");
        case FLOAT32: return std::make_shared<CodeExpression>("float");
        case FLOAT64: return std::make_shared<CodeExpression>("double");
        case BOOLEAN: return std::make_shared<CodeExpression>("bool");
        case CHAR: return std::make_shared<CodeExpression>("char");
        case DATE: return std::make_shared<CodeExpression>("uint32_t");
        case VOID_TYPE: return std::make_shared<CodeExpression>("void");
    }
    return nullptr;
}

BasicType BasicDataType::getType() {
    return type;
}

const CodeExpressionPtr BasicDataType::getTypeDefinitionCode() const {
    return std::make_shared<CodeExpression>("");
}

const DataTypePtr BasicDataType::copy() const {
    return std::make_shared<BasicDataType>(*this);
}

bool BasicDataType::operator==(const DataType& _rhs) const {
    auto rhs = dynamic_cast<const NES::BasicDataType&>(_rhs);
    return type == rhs.type && dataSize == rhs.dataSize;
}
bool BasicDataType::isUndefined() const {
    return type == UNDEFINED;
}
bool BasicDataType::isNumerical() const {
    return type == BasicType::UINT8 || type == BasicType::UINT16 || type == BasicType::UINT32
        || type == BasicType::UINT64 || type == BasicType::INT8 || type == BasicType::INT16
        || type == BasicType::INT32 || type == BasicType::INT64 || type == BasicType::FLOAT32
        || type == BasicType::FLOAT64;
}
const DataTypePtr BasicDataType::join(DataTypePtr other) const {
    // if both data types are numeric we perform an implicit conversion and use the larger type as a result.
    if (this->isNumerical() && other->isNumerical()) {
        if (getSizeBytes() > other->getSizeBytes()) {
            return createDataType(type);
        } else {
            return other;
        }
    }
    return DataType::join(other);
}

const DataTypePtr createDataType(const BasicType& type) {
    return std::make_shared<BasicDataType>(type);
}

const DataTypePtr createUndefinedDataType() {
    return std::make_shared<BasicDataType>(UNDEFINED);
}

const DataTypePtr createVarDataType(const BasicType& type, uint32_t size) {
    return std::make_shared<BasicDataType>(type, size);
}

const DataTypePtr createDataTypeVarChar(const uint32_t& size) {
    return std::make_shared<BasicDataType>(CHAR, size);
}

}// namespace NES
BOOST_CLASS_EXPORT(NES::BasicDataType);
