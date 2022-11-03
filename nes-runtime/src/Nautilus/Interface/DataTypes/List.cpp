#include <Nautilus/Interface/DataTypes/List.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
namespace NES::Nautilus {

RawList::RawList(int32_t length) {
    auto* provider = Runtime::WorkerContext::getBufferProviderTLS();
    auto optBuffer = provider->getUnpooledBuffer(length);
    NES_ASSERT2_FMT(!!optBuffer, "Cannot allocate buffer of size " << length);
    buffer = optBuffer.value();
    buffer.setNumberOfTuples(length);
}

uint32_t RawList::length() const { return buffer.getNumberOfTuples(); }

int8_t* RawList::data() { return buffer.getBuffer<int8_t>(); };

void destructList(RawList* list){//delete list;
                                 NES_DEBUG("~ " << list)}

int32_t listGetLength(const RawList& list) {
    return list.length();
}

Value<Int32> List::length() { return FunctionCall<>("listGetLength", listGetLength, this->rawReference); }

List::~List() {
    //Nautilus::FunctionCall<>("destructList", destructList, this->rawReference);
    NES_DEBUG("~List");
}

}// namespace NES::Nautilus