
#include <memory>
#include <vector>

namespace iotdb {

class AttributeField;
typedef std::shared_ptr<AttributeField> AttributeFieldPtr;

class Schema {
public:
  static Schema create();
  void addField(AttributeFieldPtr field);

private:
  Schema();
  //  Schema(const Schema&);
  //  Schema& operator=(const Schema&);
  std::vector<AttributeFieldPtr> fields;
};
}
