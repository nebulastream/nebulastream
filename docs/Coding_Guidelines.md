# General Coding Style and Clang Format
- we use the [Clickhouse style](https://clickhouse.com/docs/en/development/style)
- regarding comments, we use `///` for inline comments. `//` are there for temporarily commenting out code
- code style can be added to Clion. To set the code style follow these steps
  1. Go to the settings window: `File --> Settings`
  2. Jump to C/C++ code style: `Editor --> Code Style --> C/C++`
  3. Import the code style from `.clang-format`
  4. Click `Apply` and `OK`


# Naming Conventions and Position of Operators
- Types start with uppercase: `MyClass`
- Functions and variables start with lowercase: `myMethod`
- Constants are all upper case: `const double PI=3.14159265358979323;`
- instead of magic numbers, we use constexpr: `constexpr auto MAGIC_NUMBER = 42`
- `*` and `&` next to the type not the name, e.g., `void* p` instead of `void *p` or `void& p` instead of `void &p`.
- When using `auto`, to avoid confusion, write `auto& v` or `auto* p` to avoid ambiguity.


# Includes and Forward Declaration
- we use `include <>` for all includes 
- we avoid forward declaration wherever possible [Poll Forward Declaration](https://github.com/nebulastream/nebulastream-public/discussions/19)
- Also use [include-what-you-use](https://github.com/include-what-you-use/include-what-you-use) to reduce the includes
- never use `using namespace` in header files, as they get pulled into the namespace of all files that include the header file


# Smart Pointers and passing objects
- avoid raw memory access
  - no new calls or `malloc`
  - use smart pointers instead
- think about using shared ptr vs. unique_ptr
- shared_ptr are more expensive as one might think, as the reference count must be atomic and thread-safe
  - unique_ptr might be better suited
- think about if you have to pass the smart pointer or only the object itself [GotW #91](https://herbsutter.com/2013/06/05/gotw-91-solution-smart-pointer-parameters/)
  - access to the smart pointer or to the object itself


# OOP and Inheritance
- differentiate between structs and classes
  - classes: using features such as `private` or `protected` members
  - structs: plain-old-data structures, for example for grouping members together and passing a struct instead of members directly
- virtual destructors: make base class destructors virtual
- use final keyword in virtual functions and class declarations whenever it should not be implemented in a derived class


# Variable Declaration and Return Types
- use const as much as possible
- carefully consider return types, 
  - do not pass simple types by const ref
  - temporaries and local values --> return by value
  - getters --> reference / const reference or by value for thread safety 


# Operator Overloading
- instead of writing methods, we should overload operators that map to natural operators, e.g., `+`, `-`, `<<`, ...


# Exceptions and Error Handling
- use `std::optional<>` together with exceptions for handling errors or user input [Stroustrup Exceptions](https://www.stroustrup.com/bs_faq2.html#exceptions-why)
- link here to the design document of error handling
