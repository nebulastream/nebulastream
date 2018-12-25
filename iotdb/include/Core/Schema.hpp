
#ifndef CORE_SCHEMA_H
#define CORE_SCHEMA_H

#include <memory>
#include <vector>

namespace iotdb {

class AttributeField;
typedef std::shared_ptr<AttributeField> AttributeFieldPtr;

class Schema {
public:
  static Schema create();
  Schema& addField(AttributeFieldPtr field);
  const std::string toString() const;
private:
  Schema();
  //  Schema(const Schema&);
  //  Schema& operator=(const Schema&);
  std::vector<AttributeFieldPtr> fields;
};
}

#endif
