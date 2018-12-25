
#include <core/Schema.hpp>
#include <sstream>
#include <core/DataTypes.hpp>

namespace iotdb {

Schema Schema::create() { return Schema(); }
Schema& Schema::addField(AttributeFieldPtr field) {
  if (field)
    fields.push_back(field);
  return *this;
}

const std::string Schema::toString() const{
  std::stringstream ss;
  for(const auto& field : fields){
    ss << field->toString() << ", ";
  }
  return ss.str();
}

Schema::Schema() : fields() {}
}
