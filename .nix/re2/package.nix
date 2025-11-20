{ pkgs }:

let
  lib = pkgs.lib;
  llvm = pkgs.llvmPackages_19;
  clangStdenv = llvm.stdenv;
  libcxxStdenv = llvm.libcxxStdenv;

  build = { extraBuildInputs ? [ ], useLibcxx ? false, abseil ? null, gtest ? null, compilerFlags ? [ ], linkerFlags ? [ ] }:
    let
      stdenv = if useLibcxx then libcxxStdenv else clangStdenv;
      overrides =
        { inherit stdenv; }
        // lib.optionalAttrs (abseil != null) { abseil-cpp = abseil; }
        // lib.optionalAttrs (gtest != null) { inherit gtest; };
      base = pkgs.re2.override overrides;
      concatFlags = flags:
        if flags == [ ] then ""
        else lib.concatStringsSep " " flags;
      mergeFlags = flags: existing:
        let
          trailing = lib.optional (existing != null && existing != "") existing;
        in concatFlags (flags ++ trailing);
    in
    base.overrideAttrs (old: let oldEnv = old.env or { }; in {
      buildInputs = lib.unique ((old.buildInputs or [ ]) ++ extraBuildInputs);
      cmakeFlags = (old.cmakeFlags or [ ]) ++ [
        "-DRE2_BUILD_TESTING=OFF"
        "-DRE2_BUILD_BENCHMARK=OFF"
      ];
      doCheck = false;
      env = oldEnv // {
        NIX_CFLAGS_COMPILE = mergeFlags compilerFlags (oldEnv.NIX_CFLAGS_COMPILE or "");
      };
    });

  parseWithSanitizerArgs = arg:
    if builtins.isList arg then {
      extraBuildInputs = arg;
      useLibcxx = false;
      abseil = null;
      gtest = null;
      compilerFlags = [ ];
      linkerFlags = [ ];
    } else if builtins.isAttrs arg then {
      extraBuildInputs =
        if arg ? extraBuildInputs then arg.extraBuildInputs
        else if arg ? extraPackages then arg.extraPackages
        else [ ];
      useLibcxx = arg.useLibcxx or false;
      abseil = arg.abseil or null;
      gtest = arg.gtest or null;
      compilerFlags = arg.compilerFlags or [ ];
      linkerFlags = arg.linkerFlags or [ ];
    } else {
      extraBuildInputs = [ ];
      useLibcxx = false;
      abseil = null;
      gtest = null;
      compilerFlags = [ ];
      linkerFlags = [ ];
    };

in {
  default = build {
    abseil = pkgs.abseil-cpp;
    gtest = pkgs.gtest;
  };
  withSanitizer = arg:
    let cfg = parseWithSanitizerArgs arg;
    in build cfg;
}
