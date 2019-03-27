#ifndef API_PREDICATE_H
#define API_PREDICATE_H

#include <string>
#include <vector>

#include "../CodeGen/CodeGen.hpp"
#include "../Operators/Operator.hpp"
#include "Schema.hpp"

namespace iotdb {

class Predicate {
  public:
    Predicate(std::string fieldId, std::string value) : fieldId(fieldId), value("\"" + value + "\""){};
    Predicate(std::string fieldId, long value) : fieldId(fieldId), value(std::to_string(value)){};
    Predicate(std::string fieldId, Field& field2) : fieldId(fieldId), value("record." + field2.name){};

    std::string fieldId;
    std::string value;
    std::string to_string() { return "Predicate"; };
};

class Equal : public Predicate {
  public:
    Equal(std::string fieldId, std::string value) : Predicate(fieldId, value){};
    Equal(std::string fieldId, long value) : Predicate(fieldId, value){};
    Equal(std::string fieldId, Field& field2) : Predicate(fieldId, field2){};

    std::string to_string() { return "Equal"; };
};

class NotEqual : public Predicate {
  public:
    NotEqual(std::string fieldId, std::string value) : Predicate(fieldId, value) {}
    NotEqual(std::string fieldId, long value) : Predicate(fieldId, value) {}
    NotEqual(std::string fieldId, Field& field2) : Predicate(fieldId, field2){};

    std::string to_string() { return "NotEqual"; };
    ;
};

class Greater : public Predicate {
  public:
    Greater(std::string fieldId, std::string value) : Predicate(fieldId, value) {}
    Greater(std::string fieldId, long value) : Predicate(fieldId, value) {}
    Greater(std::string fieldId, Field& field2) : Predicate(fieldId, field2){};

    std::string to_string() { return "Greater"; };
    ;
};

class GreaterEqual : public Predicate {
  public:
    GreaterEqual(std::string fieldId, std::string value) : Predicate(fieldId, value) {}
    GreaterEqual(std::string fieldId, long value) : Predicate(fieldId, value) {}
    GreaterEqual(std::string fieldId, Field& field2) : Predicate(fieldId, field2){};
};

class Less : public Predicate {
  public:
    Less(std::string fieldId, std::string value) : Predicate(fieldId, value) {}
    Less(std::string fieldId, long value) : Predicate(fieldId, value) {}
    Less(std::string fieldId, Field& field2) : Predicate(fieldId, field2){};

    std::string to_string() { return "Less"; };
    ;
};

class LessEqual : public Predicate {
  public:
    LessEqual(std::string fieldId, std::string value) : Predicate(fieldId, value) {}
    LessEqual(std::string fieldId, long value) : Predicate(fieldId, value) {}
    LessEqual(std::string fieldId, Field& field2) : Predicate(fieldId, field2){};

    std::string to_string() { return "LessEqual"; };
    ;
};

class Like : public Predicate {
  public:
    Like(std::string fieldId, std::string value) : Predicate(fieldId, value) {}
    Like(std::string fieldId, long value) : Predicate(fieldId, value) {}
    Like(std::string fieldId, Field& field2) : Predicate(fieldId, field2){};

    std::string to_string() { return "Like"; };
    ;
};
} // namespace iotdb
#endif // API_PREDICATE_H
