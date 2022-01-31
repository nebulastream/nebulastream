#include <Exceptions/ConfigurationException.hpp>
#include <Util/Logger.hpp>
namespace NES {

ConfigurationException::ConfigurationException(const std::string& message, std::string&& stacktrace, std::experimental::source_location location)
    : NesRuntimeException(message, std::move(stacktrace), location) {

}
}// namespace NES