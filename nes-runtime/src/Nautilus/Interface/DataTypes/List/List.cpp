#include <Nautilus/Interface/DataTypes/List/List.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
namespace NES::Nautilus {

List::~List() {
    //Nautilus::FunctionCall<>("destructList", destructList, this->rawReference);
    NES_DEBUG("~List");
}

}// namespace NES::Nautilus