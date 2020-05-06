#ifndef API_INPUT_TYPES_H
#define API_INPUT_TYPES_H

#include <cassert>
#include <string>
namespace NES {

class InputType {
  public:
    enum InputTypeEnum { CSVFile,
                         BinaryFile,
                         Socket,
                         Memory,
                         UNDEFINED_INPUT_TYPE };

    InputType() : type(UNDEFINED_INPUT_TYPE){};
    InputType(InputTypeEnum type) : type(type){};

    bool operator==(const InputType& other) { return type == other.type; }

    const std::string to_string() const {
        if (type == CSVFile) {
            return std::string("CSVFile");
        } else if (type == BinaryFile) {
            return std::string("BinaryFile");
        } else if (type == Socket) {
            return std::string("BinaryFile");
        } else if (type == Memory) {
            return std::string("Memory");
        } else if (type == UNDEFINED_INPUT_TYPE) {
            return std::string("UNDEFINED_INPUT_TYPE");
        }
        assert(0 == 1 && "Input Type not defined!");
        return std::string("Should never happen.");
    }

  private:
    InputTypeEnum type;
};
}// namespace NES
#endif// API_INPUT_TYPES_H
