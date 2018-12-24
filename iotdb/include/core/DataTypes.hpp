
#include <cstdint>
#include <memory>

namespace iotdb{

  enum BasicType{INT8,
                 UINT8,
                 INT16,
                 UINT16,
                 INT32,
                 UINT32,
                 INT64,
                 UINT64,
                 FLOAT32,
                 FLOAT64,
                 BOOLEAN,
                 CHAR,
                 DATE};

  /** \brief generic implementation of DataType for ground-up support of user-defined types */
  class DataType;
  typedef std::shared_ptr<DataType> DataTypePtr;

  /** \brief represents a value of a particular DataType */
  class ValueType;
  typedef std::shared_ptr<ValueType> ValueTypePtr;

  class DataType{
      virtual ValueTypePtr getNullValue() const = 0;
      virtual uint32_t getSizeBytes() const = 0;
  };

  class AttributeField;
  typedef std::shared_ptr<AttributeField> AttributeFieldPtr;

  class AttributeField {
  public:
    AttributeField (const std::string& name, DataTypePtr data_type);
    std::string name;
    DataTypePtr data_type;
    uint32_t getFieldSize() const;
  };

  const DataTypePtr createDataType(const BasicType&);
  const DataTypePtr createDataTypeVarChar(const uint32_t& max_length);

}

