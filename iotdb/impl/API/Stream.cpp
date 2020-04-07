#include <API/Stream.hpp>
#include <API/UserAPIExpression.hpp>
#include <Util/Logger.hpp>

namespace NES {

Stream::Stream(std::string name, NES::SchemaPtr schemaPtr)
    : name(name) {
  schema = schemaPtr->copy();
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
