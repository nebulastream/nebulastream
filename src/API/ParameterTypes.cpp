
#include <API/ParameterTypes.hpp>
#include <string>

namespace NES {


const DataSourcePtr copy(const DataSourcePtr param) { return param; }
const DataSinkPtr copy(const DataSinkPtr param) { return param; }
const PredicatePtr copy(const PredicatePtr param) { return param; }
const Attributes copy(const Attributes& param) { return param; }
const MapperPtr copy(const MapperPtr param) { return param; }
const std::string toString(const DataSourcePtr param) { return "***"; }
const std::string toString(const DataSinkPtr param) { return "***"; }
const std::string toString(const PredicatePtr param) { return "***"; }
//const std::string toString(const WindowPtr& param) { return "***"; }
const std::string toString(const Attributes& param) { return "***"; }
const std::string toString(const MapperPtr param) { return "***"; }

}// namespace NES
