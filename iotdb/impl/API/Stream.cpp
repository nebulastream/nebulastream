#include <API/Stream.hpp>
#include <API/UserAPIExpression.hpp>

namespace iotdb {

Stream::Stream(std::string name, const iotdb::Schema schema)
    : name(name),
      schema(schema) {
}

Stream::Stream(std::string name, iotdb::SchemaPtr schemaPtr)
    : name(name) {
  schema.copyFields(*schemaPtr);
}

Schema& Stream::getSchema() {
  return schema;
}

Field Stream::operator[](const std::string fieldName) {
  return Field(schema.get(fieldName));
}

Field Stream::getField(const std::string fieldName) {
  std::cout << " streamName=" << name <<  " schema=" << schema.toString() << std::endl;
  return Field(schema.get(fieldName));
}

}
