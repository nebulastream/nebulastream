#include <iostream>
#include <stdexcept>
#include <sstream>


#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/export.hpp>
#include <API/Schema.hpp>
#include <Util/Logger.hpp>

namespace NES {

Schema::Schema() {
}

SchemaPtr Schema::create() {
  return std::make_shared<Schema>();
}

size_t Schema::getSize() const {
  return fields.size();
}

Schema::Schema(const SchemaPtr query) {
  copyFields(query);
}

SchemaPtr Schema::copy() const {
  return std::make_shared<Schema>(*this);
}

/* Return size of one row of schema in bytes. */
size_t Schema::getSchemaSize() const {
  size_t size = 0;
  for (auto const& field : fields) {
    size += field->getFieldSize();
  }
  return size;
}

SchemaPtr Schema::copyFields(SchemaPtr schema) {
 for(AttributeFieldPtr attr : schema->fields){
    this->fields.push_back(attr->copy());
  }
  return std::make_shared<Schema>(*this);
}

SchemaPtr Schema::addField(AttributeFieldPtr field) {
  if (field)
    fields.push_back(field);
  return std::make_shared<Schema>(*this);
}

SchemaPtr Schema::addField(const std::string& name, const BasicType& type) {
  return addField(createField(name, type));
}

SchemaPtr Schema::addField(const std::string& name, uint32_t size) {
  return addField(createField(name, size));
}

SchemaPtr Schema::addField(const std::string& name, DataTypePtr data) {
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

AttributeFieldPtr Schema::get(uint32_t index) {
  if (index < (uint32_t) fields.size()) {
    return fields[index];
  } else {
    return AttributeFieldPtr();
  }
}

bool Schema::equals(SchemaPtr schema, bool in_order) {
  if(schema->fields.size() != fields.size()) return false;
  if(in_order){
    for (int i = 0; i < fields.size(); i++){
      if(!((fields.at(i))->isEqual((schema->fields).at(i)))){
        return false;
      }
    }
    return true;
  }
  for(AttributeFieldPtr attr : fields){
    if(!(schema->get(attr->name))) return false;
    if(!(schema->get(attr->name)->getDataType()->isEqual(attr->getDataType()))) return false;
  }
  return true;
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
}

