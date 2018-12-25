
#include <core/DataTypes.hpp>
#include <sstream>

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


  class BasicDataType : public DataType{
  public:
    BasicDataType(const BasicType & _type) : type(_type){

    }
    ValueTypePtr getNullValue() const{

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
    }
    BasicType type;
  };




  const DataTypePtr createDataType(const BasicType & type){
    DataTypePtr ptr = std::make_shared<BasicDataType>(type);
    return ptr;
  }

  const DataTypePtr createDataTypeVarChar(const uint32_t &max_length){
    return DataTypePtr();
  }

}
