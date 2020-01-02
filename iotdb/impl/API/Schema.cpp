#include <iostream>
#include <stdexcept>
#include <sstream>
#include "API/Schema.hpp"

using namespace iotdb;

Schema::Schema() {
}

Schema Schema::create() {
  return Schema();
}

size_t Schema::getSize() const {
  return fields.size();
}

/* Return size of one row of schema in bytes. */
size_t Schema::getSchemaSize() const {
  size_t size = 0;
  for (auto const& field : fields) {
    size += field->getFieldSize();
  }
  return size;
}

Schema& Schema::copyFields(Schema const& schema) {
  fields.insert(fields.end(), schema.fields.begin(), schema.fields.end());
  return *this;
}

Schema& Schema::addField(AttributeFieldPtr field) {
  if (field)
    fields.push_back(field);
  return *this;
}

Schema& Schema::addField(const std::string& name, const BasicType& type) {
  return addField(createField(name, type));
}

Schema& Schema::addField(const std::string& name, uint32_t size) {
  return addField(createField(name, size));
}

Schema& Schema::addField(const std::string& name, DataTypePtr data) {
  return addField(createField(name, data));
}

// Schema &Schema::addFixSizeField(const std::string name, const APIDataType data_type) {
//  fields.emplace_back(name, data_type, data_type.defaultSize());
//  return *this;
//}

// Schema &Schema::addVarSizeField(const std::string name, const APIDataType data_type, const size_t data_size) {
//  fields.emplace_back(name, data_type, data_size);
//  return *this;
//}

AttributeFieldPtr Schema::get(const std::string pName) {
  for (auto& f : fields) {
    if (f->name == pName)
      return f;
  }
//    return AttributeFieldPtr();
  throw std::invalid_argument("field " + pName + " does not exist");
}

const AttributeFieldPtr Schema::operator[](uint32_t index) const {
  if (index < (uint32_t) fields.size()) {
    return fields[index];
  } else {

    return AttributeFieldPtr();
  }
}

const std::string Schema::toString() const {
  std::stringstream ss;
  for (auto& f : fields) {
    ss << f->toString();
  }
  ss << std::endl;
  return ss.str();
}
