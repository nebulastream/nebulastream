#ifndef API_SCHEMA_H
#define API_SCHEMA_H

#include <string>
#include <vector>
#include <memory>

#include <Core/DataTypes.hpp>

namespace iotdb{

class AttributeField;
typedef std::shared_ptr<AttributeField> AttributeFieldPtr;
typedef std::vector<AttributeFieldPtr> VecAttributeFieldPtr;

class Schema {
public:
  Schema();
  static Schema create();

  Schema &copyFields(Schema const &schema);
  Schema& addField(AttributeFieldPtr field);
  Schema& addField(const std::string &name, const BasicType &);
  //Schema &addFixSizeField(const std::string name, const APIDataType data_type);
  //Schema &addVarSizeField(const std::string name, const APIDataType data_type, const size_t data_size);
  AttributeFieldPtr get(const std::string name);
  const AttributeFieldPtr operator [](uint32_t index);


  size_t getSchemaSize() const;
  const std::string toString() const;

  std::vector<AttributeFieldPtr> fields;

};
}
#endif // API_SCHEMA_H
