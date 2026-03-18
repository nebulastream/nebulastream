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
      pname = "scope_guard";
      version = "0.9.1";

      src = fetchFromGitHub {
        owner = "Neargye";
        repo = "scope_guard";
        tag = "v${finalAttrs.version}";
        hash = "sha256-Bs8dr5t9LQ6FyM6btpDBAEvyiKoCF2H99RRFAQfx7/I=";
      };

      nativeBuildInputs = [ cmake ];
      buildInputs = extraBuildInputs;

      cmakeFlags = [
        (lib.cmakeFeature "SCOPE_GUARD_OPT_BUILD_EXAMPLES" "OFF")
        (lib.cmakeFeature "SCOPE_GUARD_OPT_BUILD_TESTS" "OFF")
        (lib.cmakeFeature "SCOPE_GUARD_OPT_INSTALL" "ON")
        (lib.cmakeFeature "CMAKE_INSTALL_INCLUDEDIR" "include")
        (lib.cmakeFeature "CMAKE_INSTALL_LIBDIR" "lib")
      ] ++ libcxxFlags;

      passthru.updateScript = nix-update-script { };

      meta = {
        description = "Scope Guard & Defer C++";
        homepage = "https://github.com/Neargye/scope_guard";
        changelog = "https://github.com/Neargye/scope_guard/releases/tag/v${finalAttrs.version}";
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
