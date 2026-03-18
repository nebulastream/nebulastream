{
  fetchFromGitHub,
  llvmPackages_19,
  lib,
  cmake,
  nix-update-script,
}:

let
  clangStdenv = llvmPackages_19.stdenv;
  libcxxStdenv = llvmPackages_19.libcxxStdenv;

  build =
    {
      extraBuildInputs ? [ ],
      useLibcxx ? false,
    }:
    let
      libcxxFlags = lib.optionals useLibcxx [
        "-DCMAKE_CXX_FLAGS=-stdlib=libc++"
        "-DCMAKE_EXE_LINKER_FLAGS=-stdlib=libc++"
        "-DCMAKE_SHARED_LINKER_FLAGS=-stdlib=libc++"
        "-DCMAKE_MODULE_LINKER_FLAGS=-stdlib=libc++"
      ];
    in
    (if useLibcxx then libcxxStdenv else clangStdenv).mkDerivation (finalAttrs: {
      pname = "reflect-cpp";
      version = "0.23.0";

      src = fetchFromGitHub {
        owner = "getml";
        repo = "reflect-cpp";
        rev = "v${finalAttrs.version}";
        hash = "sha256-TzkNu0baAeV6BdELOzyTC4P6C7gVt7e+NiXa/NhWjpo=";
      };

      nativeBuildInputs = [ cmake ];
      buildInputs = extraBuildInputs;

      cmakeFlags = [
        (lib.cmakeFeature "REFLECTCPP_INSTALL" "ON")
        (lib.cmakeFeature "REFLECTCPP_BUILD_TESTS" "OFF")
        (lib.cmakeFeature "REFLECTCPP_BUILD_BENCHMARKS" "OFF")
        (lib.cmakeFeature "REFLECTCPP_CHECK_HEADERS" "OFF")
        (lib.cmakeFeature "REFLECTCPP_USE_VCPKG" "OFF")
        (lib.cmakeFeature "REFLECTCPP_USE_BUNDLED_DEPENDENCIES" "ON")
        (lib.cmakeFeature "CMAKE_INSTALL_INCLUDEDIR" "include")
        (lib.cmakeFeature "CMAKE_INSTALL_LIBDIR" "lib")
      ] ++ libcxxFlags;

      doCheck = false;

      passthru.updateScript = nix-update-script { };

      meta = {
        description = "Reflect C++ library for C++20";
        homepage = "https://github.com/getml/reflect-cpp";
        changelog = "https://github.com/getml/reflect-cpp/releases/tag/v${finalAttrs.version}";
        license = lib.licenses.mit;
        platforms = lib.platforms.all;
      };
    });

  parseWithSanitizerArgs =
    arg:
    if builtins.isList arg then
      {
        extraBuildInputs = arg;
        useLibcxx = false;
      }
    else if builtins.isAttrs arg then
      {
        extraBuildInputs =
          if arg ? extraBuildInputs then
            arg.extraBuildInputs
          else if arg ? extraPackages then
            arg.extraPackages
          else
            [ ];
        useLibcxx = arg.useLibcxx or false;
      }
    else
      {
        extraBuildInputs = [ ];
        useLibcxx = false;
      };

in
{
  default = build { };
  withSanitizer =
    arg:
    let
      cfg = parseWithSanitizerArgs arg;
    in
    build {
      inherit (cfg) extraBuildInputs useLibcxx;
    };
}
