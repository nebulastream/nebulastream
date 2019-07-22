#include <API/Stream.hpp>
#include <API/UserAPIExpression.hpp>

namespace iotdb {

    Stream::Stream(std::string name, const iotdb::Schema schema) : name(name), schema(schema) {}

    Schema & Stream::getSchema() {return schema;}

    Field Stream::operator[](const std::string fieldName) {
        return Field(schema.get(fieldName));
    }
}