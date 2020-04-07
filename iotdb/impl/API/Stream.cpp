#include <API/Stream.hpp>
#include <API/UserAPIExpression.hpp>
#include <Util/Logger.hpp>

namespace NES {

Stream::Stream(std::string name, const NES::Schema& schema)
    : name(name),
      schema(std::make_shared<Schema>(schema)) {
}

Stream::Stream(std::string name, NES::SchemaPtr schemaPtr)
    : name(name) {
  schema = std::make_shared<Schema>();
  schema->copyFields(schemaPtr);
}

SchemaPtr Stream::getSchema() {
  return schema;
}

Field Stream::operator[](const std::string fieldName) {
  return Field(schema->get(fieldName));
}

Field Stream::getField(const std::string fieldName) {
  NES_DEBUG("getField() streamName=" << fieldName <<  " schema=" << schema->toString())
  return Field(schema->get(fieldName));
}

}
