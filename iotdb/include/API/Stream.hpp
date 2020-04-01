#ifndef STREAM_HPP
#define STREAM_HPP

#include <iostream>
#include <string>
#include <API/ParameterTypes.hpp>
#include <API/Schema.hpp>
#include <API/Stream.hpp>

namespace NES {

class Stream {
 public:
  //todo: what about this?
  Stream(std::string name, const Schema& schema);

  Stream(std::string name, SchemaPtr schema);

  Field operator[](const std::string fieldName);

  Field getField(const std::string fieldName);
  std::string getName() { return name; }

  SchemaPtr getSchema();

 private:
  std::string name;
  SchemaPtr schema;
};

typedef std::shared_ptr<Stream> StreamPtr;
}
#endif //STREAM_HPP
