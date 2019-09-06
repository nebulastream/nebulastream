#ifndef IOTDB_STREAM_HPP
#define IOTDB_STREAM_HPP

#include <iostream>
#include <string>
#include <API/ParameterTypes.hpp>
#include <API/Schema.hpp>
#include <API/Stream.hpp>

namespace iotdb {

class Stream {
 public:
  Stream(std::string name, const Schema schema);

  Field operator[](const std::string fieldName);

  std::string getName() { return name; }

  Schema &getSchema();

 private:
  std::string name;
  Schema schema;
};
}
#endif //IOTDB_STREAM_HPP
