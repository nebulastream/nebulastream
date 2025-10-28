{ pkgs, abseilCpp ? pkgs.abseil-cpp_202401, protobufBase ? pkgs.protobuf }:

let
  lib = pkgs.lib;

  # Remove default abseil from lists and inject pinned one
  rmAbsl = pkgsList:
    builtins.filter (p:
      let n = (p.pname or (p.name or "")); in n != "abseil-cpp") pkgsList;

in (protobufBase.override { "abseil-cpp" = abseilCpp; }).overrideAttrs (old: {
  buildInputs = (rmAbsl (old.buildInputs or [ ])) ++ [ abseilCpp.dev or abseilCpp ];
  propagatedBuildInputs = (rmAbsl (old.propagatedBuildInputs or [ ])) ++ [ abseilCpp ];
})
