
#include <Core/DataTypes.hpp>
#include <sstream>
#include <CodeGen/CodeExpression.hpp>

namespace iotdb {

  DataType::~DataType(){
  }

  AttributeField::AttributeField(const std::string &_name,
                                 DataTypePtr _data_type)
    : name(_name), data_type(_data_type)
  {
    //assert(data_type!=nullptr);
  }

  AttributeField::AttributeField(const std::string &_name,
                                 const BasicType &_type)
    : name(_name), data_type(createDataType(_type)){

  }

  uint32_t AttributeField::getFieldSize() const{
     return data_type->getSizeBytes();
  }

  const std::string AttributeField::toString() const{

    std::stringstream ss;
    ss << name << ":" << data_type->toString();
    return ss.str();
  }

  const AttributeFieldPtr createField(const std::string name, const BasicType & type){
    AttributeFieldPtr ptr = std::make_shared<AttributeField>(name, type);
    return ptr;
  }



  ValueType::~ValueType(){

  }

  class BasicValueType : public ValueType{
    public:
    BasicValueType(const BasicType & type, const std::string& value)
      : type_(type), value_(value){
    }

    const DataTypePtr getType() const{
      return createDataType(type_);
    }

    const CodeExpressionPtr getCodeExpression() const{
      return std::make_shared<CodeExpression>(value_);
    }

    ~BasicValueType();
  private:
    const BasicType type_;
    const std::string value_;
  };

  BasicValueType::~BasicValueType(){
  }

  class BasicDataType : public DataType{
  public:
    BasicDataType(const BasicType & _type)
      : type(_type){
    }

    ValueTypePtr getDefaultInitValue() const{
      return ValueTypePtr();
    }

    ValueTypePtr getNullValue() const{
      return ValueTypePtr();
    }
    uint32_t getSizeBytes() const{
      switch(type){
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
      }
      return 0;
    }
    const std::string toString() const{
      switch(type){
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
      }
      return "";
    }
    const CodeExpressionPtr getCode() const{
      switch(type){
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
      }
      return nullptr;
    }
    ~BasicDataType();
  private:
    BasicType type;
  };

  BasicDataType::~BasicDataType(){}



  class PointerDataType : public DataType{
  public:
    PointerDataType(const DataTypePtr& type)
      : DataType(), base_type_(type){

    }
    ValueTypePtr getDefaultInitValue() const{
         return ValueTypePtr();
       }

       ValueTypePtr getNullValue() const{
         return ValueTypePtr();
       }
       uint32_t getSizeBytes() const{
         /* assume a 64 bit architecture, each pointer is 8 bytes */
         return 8;
       }
       const std::string toString() const{
         return base_type_->toString()+"*";
       }
       const CodeExpressionPtr getCode() const{
         return std::make_shared<CodeExpression>(base_type_->getCode()->code_+"*");

       }

    virtual ~PointerDataType();
private:
    DataTypePtr base_type_;
  };

  PointerDataType::~PointerDataType(){

  }

  const DataTypePtr createDataType(const BasicType & type){
    DataTypePtr ptr = std::make_shared<BasicDataType>(type);
    return ptr;
  }

  const DataTypePtr createPointerDataType(const DataTypePtr& type){
    return std::make_shared<PointerDataType>(type);
  }

  const DataTypePtr createPointerDataType(const BasicType & type){
    return std::make_shared<PointerDataType>(createDataType(type));
  }

  const DataTypePtr createDataTypeVarChar(const uint32_t &max_length){
    return DataTypePtr();
  }

  const ValueTypePtr createBasicTypeValue(const BasicType & type, const std::string& value){

    /** \todo: create instance of datatype and add a parseValue() method to datatype, so we can check whether the value inside the string matches the type */
    return std::make_shared<BasicValueType>(type,value);
  }

}
