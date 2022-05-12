

class XClazz {
  public:
    [[example]] int addMember(int x) { return x * 2; }
};

[[example]] int addXNes(int x) { return x * 2; }
namespace NES::Test {
[[example]] int addNamespaceX(int x) { return x * 2; }
}// namespace NES::Test
