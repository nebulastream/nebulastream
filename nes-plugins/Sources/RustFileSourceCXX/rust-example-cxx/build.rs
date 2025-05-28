fn main() {
    cxx_build::bridge("src/lib.rs")
        .flag_if_supported("-std=c++23")
        .compile("rust-file-source");
}