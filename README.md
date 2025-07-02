<div align="center">
  <picture>
    <source media="(prefers-color-scheme: light)" srcset="docs/resources/NebulaBanner.png">
    <source media="(prefers-color-scheme: dark)" srcset="docs/resources/NebulaBannerDarkMode.png">
    <img alt="NebulaStream logo" src="docs/resources/NebulaBanner.png" height="100">
  </picture>
  <br />
  <!-- Badges -->
  <a href="https://github.com/nebulastream/nebulastream-public/actions/workflows/nightly.yml">
    <img src="https://github.com/nebulastream/nebulastream-public/actions/workflows/nightly.yml/badge.svg"
         alt="NES Nightly" />
  </a>
  <a href="https://bench.nebula.stream/c-benchmarks/">
    <img src="https://img.shields.io/badge/Benchmark-Conbench-blue?labelColor=3D444C"
         alt="Conbench" />
  </a>
  <a href="https://codecov.io/github/nebulastream/nebulastream" > 
    <img src="https://codecov.io/github/nebulastream/nebulastream/graph/badge.svg?token=ER83Nm1crF" alt="Codecov"/> 
  </a>  
</div>

----

NebulaStream is our attempt to develop a general-purpose, end-to-end data management system for the IoT.
It provides an out-of-the-box experience with rich data processing functionalities and a high ease-of-use.

NebulaStream is a joint research project between the DIMA group at TU Berlin and BIFOLD.

Learn more about Nebula Stream at https://www.nebula.stream or take a look at our [documentation](docs).

# Clang-Format, Clang-Tidy, License & Pragma Once Check, and Ensure Correct Comments
We use `clang-format` and `clang-tidy` to ensure code quality and consistency.
To run the checks, you can use the target `format`. 


## Development
NebulaStream targets C++23 using all features implemented in both `libc++` 18 and `libstdc++` 13.2. All tests are using
`Clang` 18.
Follow the [development guide](docs/technical/development.md) to learn how to set up the development environment.
To see our code of conduct, please refer to [CODE_OF_CONDUCT](CODE_OF_CONDUCT.md).

## Build Types
This project supports multiple build types to cater to different stages of development and deployment. Below are the details of each build type:

### Debug
- **Default Logging Level**: All logging messages are compiled.
- **Assert Checks**: Enabled.
- **Use Case**: Ideal for development, providing comprehensive logging and assert checks to help identify and fix issues.

### RelWithDebInfo (Release with Debug Information)
- **Default Logging Level**: Warning.
- **Assert Checks**: Enabled.
- **Use Case**: Balances performance and debugging, including warning-level logging and assert checks for useful debugging information without full logging overhead.

### Release
- **Default Logging Level**: Error.
- **Assert Checks**: Enabled.
- **Use Case**: Optimized for performance, with logging set to error level and assert checks disabled, ensuring only critical issues are logged.

### Benchmark
- **Logging Level**: None.
- **Assert Checks**: Disabled.
- **Use Case**: Designed for maximum performance, omitting all logging and assert checks, including null pointer checks. Suitable for thoroughly tested environments where performance is critical.
- Use this with care, as this is not regularly tested, i.e., Release terminates deterministically if a bug occurs (failed invariant/precondition), whereas Benchmark will be in an undefined state.
