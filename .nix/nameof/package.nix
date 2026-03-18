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
        (lib.cmakeFeature "CMAKE_CXX_FLAGS" "-stdlib=libc++")
        (lib.cmakeFeature "CMAKE_EXE_LINKER_FLAGS" "-stdlib=libc++")
        (lib.cmakeFeature "CMAKE_SHARED_LINKER_FLAGS" "-stdlib=libc++")
        (lib.cmakeFeature "CMAKE_MODULE_LINKER_FLAGS" "-stdlib=libc++")
      ];
    in
    (if useLibcxx then libcxxStdenv else clangStdenv).mkDerivation (finalAttrs: {
      pname = "nameof";
      version = "0.10.4";

      src = fetchFromGitHub {
        owner = "Neargye";
        repo = "nameof";
        tag = "v${finalAttrs.version}";
        hash = "sha256-g3VwgCloqFnghG7OB7aK3uB/1RNKe8h6Nu7pbIGqrEQ=";
      };

      nativeBuildInputs = [ cmake ];
      buildInputs = extraBuildInputs;

      cmakeFlags = [
        (lib.cmakeFeature "NAMEOF_OPT_BUILD_EXAMPLES" "OFF")
        (lib.cmakeFeature "NAMEOF_OPT_BUILD_TESTS" "OFF")
        (lib.cmakeFeature "NAMEOF_OPT_INSTALL" "ON")
        (lib.cmakeFeature "CMAKE_INSTALL_INCLUDEDIR" "include")
        (lib.cmakeFeature "CMAKE_INSTALL_LIBDIR" "lib")
      ] ++ libcxxFlags;

      passthru.updateScript = nix-update-script { };

      meta = {
        description = "Nameof operator for modern C++, simply obtain the name of a variable, type, function, macro, and enum";
        homepage = "https://github.com/Neargye/nameof";
        changelog = "https://github.com/Neargye/nameof/releases/tag/v${finalAttrs.version}";
        license = lib.licenses.mit;
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
