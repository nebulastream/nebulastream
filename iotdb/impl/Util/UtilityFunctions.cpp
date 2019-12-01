#include <Util/UtilityFunctions.hpp>

namespace iotdb {

// removes leading and trailing whitespaces
std::string trim(std::string s) {
  auto not_space = [](char c) { return isspace(c) == 0; };
  // trim left
  s.erase(s.begin(), find_if(s.begin(), s.end(), not_space));
  // trim right
  s.erase(find_if(s.rbegin(), s.rend(), not_space).base(), s.end());
  return s;
}

} // namespace iotdb