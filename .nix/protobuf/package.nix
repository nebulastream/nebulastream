{
  lib,
  llvmPackages_19,
  protobuf,
}:

let
  llvm = llvmPackages_19;
  clangStdenv = llvm.stdenv;
  libcxxStdenv = llvm.libcxxStdenv;

  build =
    {
      extraBuildInputs ? [ ],
      useLibcxx ? false,
      abseil ? null,
      compilerFlags ? [ ],
      linkerFlags ? [ ],
    }:
    let
      stdenv = if useLibcxx then libcxxStdenv else clangStdenv;
      overrides = {
        inherit stdenv;
      }
      // lib.optionalAttrs (abseil != null) { abseil-cpp = abseil; };
      base = protobuf.override overrides;
      concatFlags = flags: if flags == [ ] then "" else lib.concatStringsSep " " flags;
      mergeFlags =
        flags: existing:
        let
          trailing = lib.optional (existing != null && existing != "") existing;
        in
        concatFlags (flags ++ trailing);
    in
    base.overrideAttrs (
      old:
      let
        oldEnv = old.env or { };
      in
      {
        buildInputs = lib.unique ((old.buildInputs or [ ]) ++ extraBuildInputs);
        cmakeFlags = (old.cmakeFlags or [ ]) ++ [ "-Dprotobuf_BUILD_TESTS=OFF" ];
        doCheck = false;
        env = oldEnv // {
          NIX_CFLAGS_COMPILE = mergeFlags compilerFlags (oldEnv.NIX_CFLAGS_COMPILE or "");
        };
      }
    );

  parseWithSanitizerArgs =
    arg:
    if builtins.isList arg then
      {
        extraBuildInputs = arg;
        useLibcxx = false;
        abseil = null;
        compilerFlags = [ ];
        linkerFlags = [ ];
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
        abseil = arg.abseil or null;
        compilerFlags = arg.compilerFlags or [ ];
        linkerFlags = arg.linkerFlags or [ ];
      }
    else
      {
        extraBuildInputs = [ ];
        useLibcxx = false;
        abseil = null;
        compilerFlags = [ ];
        linkerFlags = [ ];
      };

in
{
  default = build { };
  withSanitizer =
    arg:
    let
      cfg = parseWithSanitizerArgs arg;
    in
    build cfg;
}
