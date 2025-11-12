use std::path::Path;

fn main() {
    // Build the native C++ library via CMake (Release profile by default).
    let dst = cmake::Config::new("native")
        .profile("Release")
        .build();

    // Propagate link search paths and static library requirements to Rust.
    println!(
        "cargo:rustc-link-search=native={}",
        dst.join("lib").display()
    );
    println!("cargo:rustc-link-lib=static=bsbe_native");
    #[cfg(target_os = "linux")]
    println!("cargo:rustc-link-lib=dylib=stdc++");
    #[cfg(target_os = "macos")]
    println!("cargo:rustc-link-lib=dylib=c++");

    let h1 = Path::new("native/include/spot_stream/BoolEnum.h");
    let h2 = Path::new("native/include/spot_sbe/BoolEnum.h");

    if !h1.exists() && !h2.exists() {
        panic!(concat!(
            "Missing SBE headers under native/include/(spot_stream|spot_sbe)/*.h\n",
            "Generate with:\n",
            "  java -jar .tools/sbe-all.jar -Dsbe.target.language=CPP ",
            "-Dsbe.output.dir=native/include -Dsbe.target.namespace=spot_stream ",
            "native/schemas/spot_sbe.xml\n",
            "and/or:\n",
            "  java -jar .tools/sbe-all.jar -Dsbe.target.language=CPP ",
            "-Dsbe.output.dir=native/include -Dsbe.target.namespace=spot_sbe ",
            "native/schemas/spot_sbe.xml\n",
            "(CI runs these in 'Generate SBE headers (C++)')\n"
        ));
    }

    println!("cargo:rerun-if-changed=native/CMakeLists.txt");
    println!("cargo:rerun-if-changed=native/include/bsbe_bridge.h");
    println!("cargo:rerun-if-changed=native/src/bsbe_bridge.cc");
    println!("cargo:rerun-if-changed=native/generated");
    println!("cargo:rerun-if-changed=native/schemas/spot_sbe.xml");
}
