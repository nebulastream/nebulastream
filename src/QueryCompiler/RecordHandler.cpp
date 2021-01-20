#include <QueryCompiler/RecordHandler.hpp>

namespace NES{

RecordHandlerPtr RecordHandler::create() {
    return std::make_shared<RecordHandler>();
}

void RecordHandler::registerAttribute(std::string name, ExpressionStatmentPtr variable){
    if(!hasAttribute(name)){
        this->variableMap[name] = variable;
    }
}

bool RecordHandler::hasAttribute(std::string name) {
    return this->variableMap.count(name) == 1;
}

ExpressionStatmentPtr RecordHandler::getAttribute(std::string name) {
    return this->variableMap[name];
}

}