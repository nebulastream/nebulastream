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
  static Schema create();

  Schema(const Schema& query);
  const SchemaPtr copy() const;

  Schema &copyFields(Schema const &schema);
  Schema &addField(AttributeFieldPtr field);
  Schema &addField(const std::string &name, const BasicType &);
  Schema &addField(const std::string &name, DataTypePtr data);
  Schema &addField(const std::string &name, uint32_t size);

  // Schema &addFixSizeField(const std::string name, const APIDataType data_type);
  // Schema &addVarSizeField(const std::string name, const APIDataType data_type, const size_t data_size);
  AttributeFieldPtr get(const std::string name);
  const AttributeFieldPtr operator[](uint32_t index) const;

  size_t getSize() const;
  size_t getSchemaSize() const;
  const std::string toString() const;

  std::vector<AttributeFieldPtr> fields;

  bool operator==(const Schema &rhs) const {
    if (fields.size() == rhs.fields.size()) {
      for (std::vector<int>::size_type i = 0; i != fields.size(); i++) {
        fields[i];
        if (!(*fields[i].get() == *rhs.fields[i].get())) {
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
