#ifndef NES_INCLUDE_UTIL_THREADNAMING_HPP_
#define NES_INCLUDE_UTIL_THREADNAMING_HPP_

namespace NES {
/**
 * @brief Sets the calling thread's name using the supplied
 * formattable string. For example, setThreadName("helper") will
 * set the thread name to "helper", setThreadName("helper-%d", 123)
 * will set the thread name to "helper-123". Be careful that on some
 * operating systems, the length of the thread name is constrained, e.g.,
 * on Linux it is 16 characters.
 * @param threadNameFmt name of the thread with formatting option
 * @param ... variadic arguments
 */
void setThreadName(const char* threadNameFmt, ...);
}

#endif//NES_INCLUDE_UTIL_THREADNAMING_HPP_
