

class XClazz {
  public:
    [[proxyfunction]] int addMember(int x) { return x * 2; }
};

[[nes::proxyfunction]] int addXNes(int x) { return x * 2; }
namespace NES::Test {
[[nes::proxyfunction]] int addNamespaceX(int x) { return x * 2; }
}// namespace NES::Test
