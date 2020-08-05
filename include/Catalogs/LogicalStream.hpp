#ifndef STREAM_HPP
#define STREAM_HPP

#include <iostream>
#include <string>

namespace NES {

class Schema;
typedef std::shared_ptr<Schema> SchemaPtr;

class LogicalStream {
  public:
    LogicalStream(std::string name, SchemaPtr schema);

    std::string getName();

    SchemaPtr getSchema();

  private:
    std::string name;
    SchemaPtr schema;
};

typedef std::shared_ptr<LogicalStream> LogicalStreamPtr;

}// namespace NES
#endif//STREAM_HPP
