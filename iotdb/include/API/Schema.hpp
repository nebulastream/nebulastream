#ifndef API_SCHEMA_H
#define API_SCHEMA_H

#include <memory>
#include <string>
#include <vector>
#include <API/Types/AttributeField.hpp>

namespace NES {
class Schema;
typedef std::shared_ptr<Schema> SchemaPtr;
class Schema {
 public:
  Schema();
  static SchemaPtr create();

  Schema(SchemaPtr query);
  const Schema& copy() const;

  SchemaPtr copyFields(SchemaPtr const schema);
  SchemaPtr addField(AttributeFieldPtr field);
  SchemaPtr addField(const std::string &name, const BasicType &);
  SchemaPtr addField(const std::string &name, DataTypePtr data);
  SchemaPtr addField(const std::string &name, uint32_t size);

  // Schema &addFixSizeField(const std::string name, const APIDataType data_type);
  // Schema &addVarSizeField(const std::string name, const APIDataType data_type, const size_t data_size);
  AttributeFieldPtr get(const std::string name);
  AttributeFieldPtr get(uint32_t index);

  size_t getSize() const;
  size_t getSchemaSize() const;
  const std::string toString() const;

  std::vector<AttributeFieldPtr> fields;

  bool operator==(const SchemaPtr rhs) const {
    if (fields.size() == rhs->fields.size()) {
      for (std::vector<int>::size_type i = 0; i != fields.size(); i++) {
        fields[i];
        // schemas are equal, if their attributes are equal, right? So lets check this:
        if(!(fields[i]->isEqual(rhs->fields[i]))){
            return false;
        }
        if (!(fields[i].get() == rhs->fields[i].get())) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  friend class boost::serialization::access;
  template<class Archive>
  void serialize(Archive &ar, const unsigned int version) {
    ar & fields;
  }
};

}  // namespace NES
#endif // API_SCHEMA_H
