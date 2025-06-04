#include <unordered_map>


namespace NES::SERVER
{

struct Sink
{
    std::string name;
    std::string type;
    std::unordered_map<std::string, std::string> config;
};

}
