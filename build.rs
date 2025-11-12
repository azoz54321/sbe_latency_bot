use std::path::Path;

fn main() {
    let h1 = Path::new("native/include/spot_stream/BoolEnum.h");
    let h2 = Path::new("native/include/spot_sbe/BoolEnum.h");
    if !h1.exists() && !h2.exists() {
        panic!(
            "Missing SBE headers under native/include/(spot_stream|spot_sbe)/*.h\n"
                "Generate with:\n  java -jar .tools/sbe-all.jar -Dsbe.target.language=CPP -Dsbe.output.dir=native/include -Dsbe.target.namespace=spot_stream native/schemas/spot_sbe.xml\n"
                "and/or:\n  java -jar .tools/sbe-all.jar -Dsbe.target.language=CPP -Dsbe.output.dir=native/include -Dsbe.target.namespace=spot_sbe native/schemas/spot_sbe.xml\n"
                "(CI runs these in 'Generate SBE headers (C++)')"
        );
    }

    let dst = cmake::Config::new("native").build();
    println!(
        "cargo:rustc-link-search=native={}",
        dst.join("lib").display()
    );
    println!("cargo:rustc-link-lib=static=bsbe_native");
    #[cfg(target_env = "gnu")]
    println!("cargo:rustc-link-lib=dylib=stdc++");
    println!("cargo:rerun-if-changed=native/CMakeLists.txt");
    println!("cargo:rerun-if-changed=native/include/bsbe_bridge.h");
    println!("cargo:rerun-if-changed=native/src/bsbe_bridge.cc");
    println!("cargo:rerun-if-changed=schemas/binance_sbe.xml");
    println!("cargo:rerun-if-changed=native/generated");
    println!("cargo:rerun-if-changed=native/schemas/spot_sbe.xml");
}
