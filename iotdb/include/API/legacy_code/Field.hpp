#ifndef API_FIELD_H
#define API_FIELD_H

#include "APIDataTypes.hpp"
#include <stdexcept>
#include <string>
namespace iotdb {

class Field {
  public:
    Field(std::string name, APIDataType data_type, std::size_t data_size)
        : name(name), data_type(data_type), data_size(data_size){};
    std::string name;
    APIDataType data_type;
    std::size_t data_size;

    size_t getFieldSize() const { return data_size; };
};
} // namespace iotdb
#endif // API_FIELD_H
