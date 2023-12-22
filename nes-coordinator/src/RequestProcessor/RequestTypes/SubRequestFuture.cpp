#include <RequestProcessor/RequestTypes/SubRequestFuture.hpp>
#include <RequestProcessor/RequestTypes/AbstractSubRequest.hpp>

namespace NES::RequestProcessor {
SubRequestFuture::SubRequestFuture(AbstractSubRequestPtr request, std::future<std::any> future)
    : request(std::move(request)), future(std::move(future)) {}

std::any SubRequestFuture::get() {
    request->execute();
    return future.get();
}
}