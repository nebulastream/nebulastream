#ifndef NES_STORAGEHANDLERACQUIRERESOURCESEXCEPTION_HPP
#define NES_STORAGEHANDLERACQUIRERESOURCESEXCEPTION_HPP
#include <Exceptions/RequestExecutionException.hpp>

namespace NES::Exceptions {
class StorageHandlerAcquireResourcesException : public RequestExecutionException {
  public:
    explicit StorageHandlerAcquireResourcesException(std::string);

    [[nodiscard]] const char * what() const noexcept override;
  private:
    std::string message;
};
}

#endif//NES_STORAGEHANDLERACQUIRERESOURCESEXCEPTION_HPP
