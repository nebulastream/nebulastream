{ pkgs }:
let
  lib = pkgs.lib;
  llvm = pkgs.llvmPackages_19;
  clangStdenv = llvm.stdenv;
  libcxxStdenv = llvm.libcxxStdenv;

  build = { extraBuildInputs ? [ ], useLibcxx ? false }:
    let
      libcxxFlags = lib.optionals useLibcxx [
        "-DCMAKE_CXX_FLAGS=-stdlib=libc++"
        "-DCMAKE_EXE_LINKER_FLAGS=-stdlib=libc++"
        "-DCMAKE_SHARED_LINKER_FLAGS=-stdlib=libc++"
        "-DCMAKE_MODULE_LINKER_FLAGS=-stdlib=libc++"
      ];
    in
    (if useLibcxx then libcxxStdenv else clangStdenv).mkDerivation (finalAttrs: {
      pname = "nlohmann_json";
      version = "3.12.0";

      src = pkgs.fetchFromGitHub {
        owner = "nlohmann";
        repo = "json";
        rev = "v${finalAttrs.version}";
        hash = "sha256-cECvDOLxgX7Q9R3IE86Hj9JJUxraDQvhoyPDF03B2CY=";
      };

      patches = [ ./patches/0001-fix-missing-cstdio-header-for-missing-EOF.patch ];

      nativeBuildInputs = [ pkgs.cmake ];
      buildInputs = extraBuildInputs;

      libcxxFlags = lib.optionals useLibcxx [
        "-DCMAKE_CXX_FLAGS=-stdlib=libc++ -isystem ${llvm.libcxx}/include/c++/v1"
        "-DCMAKE_EXE_LINKER_FLAGS=-stdlib=libc++"
        "-DCMAKE_SHARED_LINKER_FLAGS=-stdlib=libc++"
        "-DCMAKE_MODULE_LINKER_FLAGS=-stdlib=libc++"
      ];

      cmakeFlags = [
        "-DCMAKE_INSTALL_INCLUDEDIR=include"
        "-DJSON_BuildTests=OFF"
        "-DJSON_FastTests=OFF"
        "-DJSON_MultipleHeaders=ON"
      ] ++ libcxxFlags;

      strictDeps = true;

      meta = with lib; {
        description = "JSON for Modern C++";
        homepage = "https://json.nlohmann.me";
        changelog = "https://github.com/nlohmann/json/blob/develop/ChangeLog.md";
        license = licenses.mit;
        platforms = platforms.all;
      };
    });

  parseWithSanitizerArgs = arg:
    if builtins.isList arg then {
      extraBuildInputs = arg;
      useLibcxx = false;
    } else if builtins.isAttrs arg then {
      extraBuildInputs =
        if arg ? extraBuildInputs then arg.extraBuildInputs
        else if arg ? extraPackages then arg.extraPackages
        else [ ];
      useLibcxx = arg.useLibcxx or false;
    } else {
      extraBuildInputs = [ ];
      useLibcxx = false;
    };

in {
  default = build { };
  withSanitizer = arg:
    let cfg = parseWithSanitizerArgs arg;
    in build {
      inherit (cfg) extraBuildInputs useLibcxx;
    };
}
