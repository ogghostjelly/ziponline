use std::{fs, io, str::FromStr};

use reqwest::{Client, Url};
use ziponline::extract_file;

#[tokio::main]
async fn main() {
    let client = Client::new();
    // NOTE: edge.forgecdn.net urls will redirect to mediafilez and fail if used directly,
    //       so we must use the actual url to download from.
    let url =
        Url::from_str("https://mediafilez.forgecdn.net/files/5838/779/create-1.20.1-0.5.1.j.jar")
            .unwrap();
    let total = std::time::Instant::now();
    let mut reader = extract_file(&client, &url, None, "META-INF/mods.toml")
        .await
        .unwrap();
    let start = std::time::Instant::now();
    io::copy(&mut reader, &mut fs::File::create("example.json").unwrap()).unwrap();
    println!("Wrote file in {:?}", std::time::Instant::now() - start);
    println!("Finished in {:?}", std::time::Instant::now() - total);
}
