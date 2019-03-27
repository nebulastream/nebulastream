#ifndef API_SOURCE_H
#define API_SOURCE_H

#include <string>

#include "InputTypes.hpp"

using namespace std;

namespace iotdb {

enum SourceType { Stream, Rest };

class Source {
  public:
    static Source create();
    Source();
    Source& inputType(InputType pType);
    Source& sourceType(SourceType sType);

    Source& path(string path);
    InputType& getType();
    string& getPath();

  private:
    InputType typeValue;
    SourceType srcType;
    string pathValue;
};
} // namespace iotdb

#endif /* INCLUDE_API_SOURCE_H_ */
