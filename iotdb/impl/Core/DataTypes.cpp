
#include <CodeGen/CodeExpression.hpp>
#include <Core/DataTypes.hpp>
#include <sstream>
#include <vector>

namespace iotdb {

DataType::DataType(){

}

DataType::DataType(const DataType&){

}

DataType& DataType::operator=(const DataType&){
  return *this;
}

DataType::~DataType() {}

AttributeField::AttributeField(const std::string& _name, DataTypePtr _data_type) : name(_name), data_type(_data_type)
{
    // assert(data_type!=nullptr);
}

AttributeField::AttributeField(const std::string& _name, const BasicType& _type)
    : name(_name), data_type(createDataType(_type))
{
}

AttributeField::AttributeField(const std::string& _name, uint32_t _size)
    : name(_name), data_type(createDataTypeVarChar(_size))
{
}
uint32_t AttributeField::getFieldSize() const { return data_type->getSizeBytes(); }

const DataTypePtr AttributeField::getDataType() const { return data_type; }

const std::string AttributeField::toString() const
{

    std::stringstream ss;
    ss << name << ":" << data_type->toString();
    return ss.str();
}

const AttributeFieldPtr AttributeField::copy() const{
  return std::make_shared<AttributeField>(*this);
}

bool AttributeField::isEqual(const AttributeField& attr){
  if(attr.name==name && attr.data_type->toString() == data_type->toString()){
      return true;
  }
  return false;
}

bool AttributeField::isEqual(const AttributeFieldPtr& attr){
  if(!attr) return false;
  return this->isEqual(attr);
}

AttributeField::AttributeField(const AttributeField& other)
  : name(other.name), data_type(other.data_type->copy())
{
}

AttributeField& AttributeField::operator=(const AttributeField& other){
  if(this!=&other){
      this->name=other.name;
      this->data_type=other.data_type->copy();
  }
  return *this;
}

const AttributeFieldPtr createField(const std::string name, const BasicType& type)
{
    AttributeFieldPtr ptr = std::make_shared<AttributeField>(name, type);
    return ptr;
}

const AttributeFieldPtr createField(const std::string name, uint32_t size)
{
    AttributeFieldPtr ptr = std::make_shared<AttributeField>(name, size);
    return ptr;
}

ValueType::ValueType(){

}
ValueType::ValueType(const ValueType&){

}
ValueType& ValueType::operator=(const ValueType&){
  return *this;
}

ValueType::~ValueType() {}

class BasicValueType : public ValueType {
  public:
    BasicValueType(const BasicType& type, const std::string& value) : type_(type), value_(value){};

    const DataTypePtr getType() const override{ return createDataType(type_); }

    const CodeExpressionPtr getCodeExpression() const override{ return std::make_shared<CodeExpression>(value_); }

    const ValueTypePtr copy() const override{
      return std::make_shared<BasicValueType>(*this);
    }

    const bool isArrayValueType() const { return false; }

    ~BasicValueType() override;

  private:
    const BasicType type_;
    std::string value_;
};

BasicValueType::~BasicValueType() {}

class BasicDataType : public DataType {
  public:
    BasicDataType(const BasicType& _type) : type(_type), dataSize(0) {}

    BasicDataType(const BasicType& _type, uint32_t _size) : type(_type), dataSize(_size) {}

    ValueTypePtr getDefaultInitValue() const  override{ return ValueTypePtr(); }

    ValueTypePtr getNullValue() const  override{ return ValueTypePtr(); }

    uint32_t getSizeBytes() const override
    {
        if (dataSize == 0) {
            return getFixSizeBytes();
        }
        else {
            return getFixSizeBytes() * dataSize;
        }
    }

    uint32_t getFixSizeBytes() const
    {
        switch (type) {
        case INT8:
            return sizeof(int8_t);
        case UINT8:
            return sizeof(uint8_t);
        case INT16:
            return sizeof(int16_t);
        case UINT16:
            return sizeof(uint16_t);
        case INT32:
            return sizeof(int32_t);
        case UINT32:
            return sizeof(uint32_t);
        case INT64:
            return sizeof(int64_t);
        case UINT64:
            return sizeof(uint64_t);
        case FLOAT32:
            return sizeof(float);
        case FLOAT64:
            return sizeof(double);
        case BOOLEAN:
            return sizeof(bool);
        case CHAR:
            return sizeof(char);
        case DATE:
            return sizeof(uint32_t);
        case VOID_TYPE:
            return 0;
        }
        return 0;
    }
    const std::string toString() const override
    {
        switch (type) {
        case INT8:
            return "INT8";
        case UINT8:
            return "UINT8";
        case INT16:
            return "INT16";
        case UINT16:
            return "UINT16";
        case INT32:
            return "INT32";
        case UINT32:
            return "UINT32";
        case INT64:
            return "INT64";
        case UINT64:
            return "UINT64";
        case FLOAT32:
            return "FLOAT32";
        case FLOAT64:
            return "FLOAT64";
        case BOOLEAN:
            return "BOOLEAN";
        case CHAR:
            return "CHAR";
        case DATE:
            return "DATE";
        case VOID_TYPE:
            return "VOID";
        }
        return "";
    }

    const std::string convertRawToString(void* data) const override
    {
        if (!data)
            return std::string();
        std::stringstream str;
        switch (type) {
        case INT8:
            return std::to_string(*reinterpret_cast<int8_t*>(data));
        case UINT8:
            return std::to_string(*reinterpret_cast<uint8_t*>(data));
        case INT16:
            return std::to_string(*reinterpret_cast<int16_t*>(data));
        case UINT16:
            return std::to_string(*reinterpret_cast<uint16_t*>(data));
        case INT32:
            return std::to_string(*reinterpret_cast<int32_t*>(data));
        case UINT32:
            return std::to_string(*reinterpret_cast<uint32_t*>(data));
        case INT64:
            return std::to_string(*reinterpret_cast<int64_t*>(data));
        case UINT64:
            return std::to_string(*reinterpret_cast<uint64_t*>(data));
        case FLOAT32:
            return std::to_string(*reinterpret_cast<float*>(data));
        case FLOAT64:
            return std::to_string(*reinterpret_cast<double*>(data));
        case BOOLEAN:
            return std::to_string(*reinterpret_cast<bool*>(data));
        case CHAR:
            if (dataSize == 0) {
                return std::to_string(*reinterpret_cast<char*>(data));
            }
            else {
                return std::string(reinterpret_cast<char*>(data));
            }
        case DATE:
            return std::to_string(*reinterpret_cast<uint32_t*>(data));
        case VOID_TYPE:
            return "";
        }
        return "";
    }

    const CodeExpressionPtr getCode() const override
    {
        switch (type) {
        case INT8:
            return std::make_shared<CodeExpression>("int8_t");
        case UINT8:
            return std::make_shared<CodeExpression>("uint8_t");
        case INT16:
            return std::make_shared<CodeExpression>("int16_t");
        case UINT16:
            return std::make_shared<CodeExpression>("uint16_t");
        case INT32:
            return std::make_shared<CodeExpression>("int32_t");
        case UINT32:
            return std::make_shared<CodeExpression>("uint32_t");
        case INT64:
            return std::make_shared<CodeExpression>("int64_t");
        case UINT64:
            return std::make_shared<CodeExpression>("uint64_t");
        case FLOAT32:
            return std::make_shared<CodeExpression>("float");
        case FLOAT64:
            return std::make_shared<CodeExpression>("double");
        case BOOLEAN:
            return std::make_shared<CodeExpression>("bool");
        case CHAR:
            return std::make_shared<CodeExpression>("char");
        case DATE:
            return std::make_shared<CodeExpression>("uint32_t");
        case VOID_TYPE:
            return std::make_shared<CodeExpression>("void");
        }
        return nullptr;
    }

    const CodeExpressionPtr getTypeDefinitionCode() const override{ return std::make_shared<CodeExpression>(""); }

    const DataTypePtr copy() const override{
      return std::make_shared<BasicDataType>(*this);
    }

    ~BasicDataType() override;

  private:
    BasicType type;
    uint32_t dataSize;
};

BasicDataType::~BasicDataType() {}

class PointerDataType : public DataType {
  public:
    PointerDataType(const DataTypePtr& type) : DataType(), base_type_(type) {}
    ValueTypePtr getDefaultInitValue() const override{ return ValueTypePtr(); }

    ValueTypePtr getNullValue() const override{ return ValueTypePtr(); }
    uint32_t getSizeBytes() const override
    {
        /* assume a 64 bit architecture, each pointer is 8 bytes */
        return 8;
    }
    const std::string toString() const override{ return base_type_->toString() + "*"; }
    const std::string convertRawToString(void* data) const override
    {
        if (!data)
            return "";
        return "POINTER"; // std::to_string(data);
    }
    const CodeExpressionPtr getCode() const override
    {
        return std::make_shared<CodeExpression>(base_type_->getCode()->code_ + "*");
    }
    const CodeExpressionPtr getTypeDefinitionCode() const override{ return base_type_->getTypeDefinitionCode(); }

    const DataTypePtr copy() const override{
      return std::make_shared<PointerDataType>(*this);
    }

    virtual ~PointerDataType() override;

  private:
    DataTypePtr base_type_;
};

PointerDataType::~PointerDataType() {}

const DataTypePtr createDataType(const BasicType& type)
{
    DataTypePtr ptr = std::make_shared<BasicDataType>(type);
    return ptr;
}

const DataTypePtr createVarDataType(const BasicType& type, uint32_t size)
{
    DataTypePtr ptr = std::make_shared<BasicDataType>(type, size);
    return ptr;
}

const DataTypePtr createPointerDataType(const DataTypePtr& type) { return std::make_shared<PointerDataType>(type); }

const DataTypePtr createPointerDataType(const BasicType& type)
{
    return std::make_shared<PointerDataType>(createDataType(type));
}

const DataTypePtr createDataTypeVarChar(const uint32_t& size)
{
    DataTypePtr ptr = std::make_shared<BasicDataType>(CHAR, size);
    return ptr;
}

const ValueTypePtr createBasicTypeValue(const BasicType& type, const std::string& value)
{

    /** \todo: create instance of datatype and add a parseValue() method to datatype, so we can check whether the value
     * inside the string matches the type */
    return std::make_shared<BasicValueType>(type, value);
}




//todo: ------------------------------

/**
 * class ArrayValueType keeps a field of values of basic types
 */
    class ArrayValueType : public ValueType {
    public:
        ArrayValueType(const BasicType& type, const std::vector<std::string>& value) : type_(type), value_(value){};
        ArrayValueType(const BasicType& type, const std::string& value) : type_(type), isString_(true){
            value_.push_back(value);
        };

        const DataTypePtr getType() const override{ return createDataType(type_); }

        const CodeExpressionPtr getCodeExpression() const override{
            if(isString_) return std::make_shared<CodeExpression>(value_.at(0));
            std::stringstream str;
            str << "{";
            for(int i = 0; i < value_.size(); i++){
                if(i != 0) str << ", ";
                str << value_.at(i);
            }
            str << "}";
            return std::make_shared<CodeExpression>(str.str());
        }

        const ValueTypePtr copy() const override{
            return std::make_shared<ArrayValueType>(*this);
        }

        const bool isArrayValueType() const { return true; }

        ~ArrayValueType() override;

    private:
        const BasicType type_;
        bool isString_ = false;
        std::vector<std::string> value_;
    };

    ArrayValueType::~ArrayValueType() {}

/**
 * creates a "string"-value (means char *)
 * @param value : std:string : the string value
 * @return ValueTypePtr : the structure keeping the given values (-- here it keeps it as a single string)
 */
const ValueTypePtr createStringTypeValue(const std::string& value, bool stringFlag)
{
    std::stringstream str;
    if(stringFlag) str << "\"" ;
    str << value;
    if(stringFlag) str << "\"";
    return std::make_shared<ArrayValueType>(BasicType::CHAR, str.str());
}

} // namespace iotdb
