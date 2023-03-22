use mongodb::bson::{Document, RawDocument};
use mongodb::{bson, options::ClientOptions, Client};
use serde_json::Value;
use std::fmt::Debug;
use std::fs::File;
use std::io::prelude::*;
use structopt::StructOpt;

#[derive(StructOpt)]
#[structopt(
    name = "rust_mongo_cli_tool",
    about = "Imports JSON file to MongoDB, runs a query, and exports results to a file"
)]
struct Opt {
    #[structopt(parse(from_os_str))]
    json: std::path::PathBuf,

    #[structopt(parse(from_os_str))]
    query: std::path::PathBuf,

    #[structopt(parse(from_os_str))]
    output: std::path::PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opt = Opt::from_args();

    let json_file = opt.json;
    let query_file = opt.query;
    let output_file = opt.output;

    // Connect to MongoDB
    let client_options = ClientOptions::parse("mongodb://localhost:27017").await?;
    let client = Client::with_options(client_options)?;
    let db = client.database("my_db");
    let collection = db.collection("temp_collection");

    // Read JSON file and insert data into MongoDB
    let mut file = File::open(json_file)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    let data: Value = serde_json::from_str(&contents)?;
    let documents = data.as_array().expect("Failed to parse JSON as array");

    let mut insert_docs = vec![];
    for doc in documents {
        insert_docs.push(doc.clone());
    }
    collection.insert_many(insert_docs, None).await?;

    // Read and execute the query file
    let mut query_contents = String::new();
    File::open(query_file)?.read_to_string(&mut query_contents)?;
    let query: Vec<Value> = serde_json::from_str(&query_contents)?;

    // Convert JSON values to BSON documents
    let pipeline: Vec<Document> = query
        .into_iter()
        .map(|value| {
            bson::to_bson(&value)
                .unwrap()
                .as_document()
                .unwrap()
                .clone()
        })
        .collect();

    let mut cursor = collection.aggregate(pipeline, None).await?;

    // Save results to the output file
    let mut results: Vec<Document> = Vec::new();

    while cursor.advance().await? {
        let doc = cursor.current();
        let mut doc = Document::from(bson::to_bson(&doc).unwrap().as_document().unwrap().clone());
        dbg!(&doc);
        doc.remove("_id").expect("Could not remove _id field");
        results.push(doc);
    }

    let results_json = serde_json::to_string_pretty(&results)?;
    std::fs::write(output_file, results_json)?;

    // Delete imported collection
    collection.drop(None).await?;

    println!("Task completed successfully!");
    Ok(())
}
