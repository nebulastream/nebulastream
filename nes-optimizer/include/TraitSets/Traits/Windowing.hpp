/// TODO: why windowing cant be a trait -> in the end all subqueries need to have the same traitset, or?
// one way to model it would be to remove the windowing trait by including the window operator.


#include <string>

namespace NES
{

// This lets us propagate window properties through the plan.
struct Windowing
{
    std::string windowType;
    int size;
};

}
