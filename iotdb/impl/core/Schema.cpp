
#include <core/Schema.hpp>

namespace iotdb {

Schema Schema::create() { return Schema(); }
void Schema::addField(AttributeFieldPtr field) {
  if (field)
    fields.push_back(field);
}

Schema::Schema() : fields() {}
}
