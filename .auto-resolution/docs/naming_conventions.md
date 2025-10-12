This document covers the naming convention used within the nebulastream system.
Naming is complex, and thus, it is especially important to follow a fixed naming scheme when choosing names for new components and extensions of existing components.

This document covers naming conventions of types (mainly classes and structs) while coding guidelines for other language constructs are handled via clang-tidy.

We only use acronyms in the type name if they are widely used outside of the nebulastream system. (E.g., CSV, JSON).

Types implementing an interface follow a naming schema of `{NameOfSpecialization}{NameOfInterface}`.

```c++
class Source {
    virtual void source_around() = 0;
};

class SpecificSource : public Source {
    void source_around() override { specifc_sourcing_around(); }
};

```

Interfaces inheriting from other interfaces may choose to replace the original interface name.
Inheriting from multiple non-mixin type interfaces, usually requires to come up with a new name.


```c++
class Node {
    ///...
};

class Operator : public Node {
    /// pure virtual
};

class JoinOperator : public Operator {
    /// concrete
};

class FunctionNode : public Node {
    /// pure virtual
}; 

class AddFunctionNode : public FunctionNode {
    /// concrete
}; 

/// multiple inheritance with a real interface and a mixin
class SubFunctionNode : public FunctionNode, public std::enabled_shared_from_this<SubFunctionNode> {
    /// concrete
}; 

/// multiple inheritance with two real interfaces.
class BufferManager : public AbstractBufferProvider, public AbstractPoolProvider {
    /// concrete
}; 

```

The alternative `{NameOfInterface}{NameOfSpecialization}` would potentially create ambiguous names, e.g., `SourceFile` instead of `FileSource.`
Also the alternative is very unconventional in general programming jargon the standard library does not use `ListLinked` or `MapUnordered`
