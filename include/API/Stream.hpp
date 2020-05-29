#ifndef STREAM_HPP
#define STREAM_HPP

#include <API/ParameterTypes.hpp>
#include <API/Schema.hpp>
#include <API/Stream.hpp>
#include <iostream>
#include <string>

namespace NES {

class Stream {
  public:
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
}// namespace NES
#endif//STREAM_HPP
