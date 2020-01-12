#include <API/Stream.hpp>
#include <API/UserAPIExpression.hpp>
#include <Util/Logger.hpp>

namespace NES {

Stream::Stream(std::string name, const NES::Schema schema)
    : name(name),
      schema(schema) {
}

Stream::Stream(std::string name, NES::SchemaPtr schemaPtr)
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
  IOTDB_DEBUG("getField() streamName=" << fieldName <<  " schema=" << schema.toString())
  return Field(schema.get(fieldName));
}

}
