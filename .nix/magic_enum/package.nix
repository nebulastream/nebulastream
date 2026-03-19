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
      pname = "magic-enum";
      version = "0.9.7";

      src = fetchFromGitHub {
        owner = "Neargye";
        repo = "magic_enum";
        rev = "d468f23408f1207a4c63b370bb34e4cfabffad83";
        hash = "sha256-KSP84pQ6cceohquLvU7cn/vaxnz/AacUSIHBAe4Vqas=";
      };

      patches = [ ./patches/0001-disable-magic-enum-value.patch ];

      nativeBuildInputs = [ cmake ];
      buildInputs = extraBuildInputs;

      cmakeFlags = [
        (lib.cmakeFeature "MAGIC_ENUM_OPT_BUILD_TESTS" "OFF")
        (lib.cmakeFeature "MAGIC_ENUM_OPT_BUILD_EXAMPLES" "OFF")
        (lib.cmakeFeature "MAGIC_ENUM_OPT_INSTALL" "ON")
        (lib.cmakeFeature "CMAKE_INSTALL_INCLUDEDIR" "include")
        (lib.cmakeFeature "CMAKE_INSTALL_LIBDIR" "lib")
      ] ++ libcxxFlags;

      passthru.updateScript = nix-update-script { };

      meta = {
        description = "Static reflection for enums";
        homepage = "https://github.com/Neargye/magic_enum";
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
