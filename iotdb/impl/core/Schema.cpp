
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
  for(size_t i=0;i<fields.size();++i){
    ss << fields[i]->toString();
    if(i+1<fields.size())
      ss << ", ";
  }
  return ss.str();
}

Schema::Schema() : fields() {}

}
