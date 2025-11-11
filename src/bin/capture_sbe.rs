use anyhow::{anyhow, Context};
use std::env;
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::PathBuf;

fn main() -> anyhow::Result<()> {
    let mut args = env::args().skip(1);
    let Some(output) = args.next() else {
        return Err(anyhow!("usage: capture_sbe <output.bin>"));
    };

    let mut stdin = io::stdin().lock();
    let mut buffer = Vec::new();
    stdin
        .read_to_end(&mut buffer)
        .context("reading stdin for capture")?;
    if buffer.is_empty() {
        return Err(anyhow!("no bytes received on stdin"));
    }

    let path = PathBuf::from(output);
    let mut file = File::create(&path).with_context(|| format!("create {path:?}"))?;
    file.write_all(&buffer)
        .with_context(|| format!("write {path:?}"))?;
    eprintln!("captured {} bytes to {}", buffer.len(), path.display());
    Ok(())
}
