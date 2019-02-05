#ifndef API_SCHEMA_H
#define API_SCHEMA_H

#include <string>
#include <vector>
#include <memory>

#include "API/Field.hpp"
#include "API/Source.hpp"

namespace iotdb{

class AttributeField;
typedef std::shared_ptr<AttributeField> AttributeFieldPtr;

class Schema {
public:
  Schema();
  static Schema create();

  Schema &copyFields(Schema const &schema);
  Schema& addField(AttributeFieldPtr field);
  Schema &addFixSizeField(const std::string name, const APIDataType data_type);
  Schema &addVarSizeField(const std::string name, const APIDataType data_type, const size_t data_size);
  Field &get(const std::string name);

  size_t getSchemaSize() const;
  const std::string toString() const;

  std::vector<Field> fields;

private:
  Schema(const SourceType source_type);
};
}
#endif // API_SCHEMA_H
