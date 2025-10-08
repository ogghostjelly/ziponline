use std::{fs, io, str::FromStr};

use reqwest::{Client, Url};
use ziponline::extract_file;

#[tokio::main]
async fn main() {
    let client = Client::new();
    let url = Url::from_str("https://cdn.modrinth.com/data/Xbc0uyRg/versions/XMiAOQvM/create-fabric-0.5.1-i-build.1630%2Bmc1.19.2.jar").unwrap();
    let total = std::time::Instant::now();
    let mut reader = extract_file(&client, &url, None, "fabric.mod.json")
        .await
        .unwrap();
    let start = std::time::Instant::now();
    io::copy(&mut reader, &mut fs::File::create("example.json").unwrap()).unwrap();
    println!("Wrote file in {:?}", std::time::Instant::now() - start);
    println!("Finished in {:?}", std::time::Instant::now() - total);
}
