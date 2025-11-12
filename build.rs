use std::path::Path;

fn main() {
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

    println!("cargo:rerun-if-changed=native/schemas/spot_sbe.xml");
}
