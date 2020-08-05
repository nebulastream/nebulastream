#ifndef STREAM_HPP
#define STREAM_HPP

#include <iostream>
#include <string>

namespace NES {

class Schema;
typedef std::shared_ptr<Schema> SchemaPtr;

/**
 * @brief The LogicalStream wraps the stream name and the schema.
 */
class LogicalStream {
  public:
    LogicalStream(std::string name, SchemaPtr schema);

    /**
     * @brief Gets the stream name
     */
    std::string getName();

    /**
     * @brief Gets the stream schema
     */
    SchemaPtr getSchema();

  private:
    std::string name;
    SchemaPtr schema;
};

typedef std::shared_ptr<LogicalStream> LogicalStreamPtr;

}// namespace NES
#endif//STREAM_HPP
