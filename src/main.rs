use std::fs::File;
use std::io::{Write, BufRead, BufReader};
use std::time::Duration;
use std::sync::{Arc, Mutex};
use std::thread;

use async_std::task;
use serde::{Deserialize, Serialize};
use tokio::time::sleep;
use async_std::future;
use tokio_tungstenite::tungstenite::Messege;
use secp256k1::{Messege, Secp256k1, SecretKey, Signature};

//initiatung 5 clients
const NUM_CLIENTS: usize =5;

#[derive(Debug, Serialize, Deserialize)]
struct CacheData {
    average_price: f64,
    data_points: Vec<f64>,
    // initiating signiture
    signiture: Vec<u8>,
}

struct AggregatorDAta {
    //initiating average vector
    averages: Vec<f64>,
}


// adding signiture parameters to the function as per Q3
async fn cache_mode(times: u64,
                    output_file: &str,
                    aggredator_tx: tokio::sync::mpsc::Sender<(f64, Vec<u8>)>,
                    secp: Arc<Mutex<Secp256k1>>,
                secret_key: SecretKey,
            ) {
    let mut data_points = Vec::new();
    let websocket_url = "wss://stream.binance.com:9443/ws/btcusdt@trade";

    for _ in 0..times {
        let (ws_stream, _) = tokio_tungstenite::connect_async(websocket_url)
            .await
            .expect("Error connecting to WebSocket");

        let handle_message = |message: Result<_, _>| {
            match message {
                Ok(msg) => {
                    if let Ok(text) = msg.to_text() {
                         let data: serde_json::Value = serde_json::from_str(&msg.to_string()).unwrap();
                        if let Some(symbol) = data["s"].as_str() {
                        if symbol == "BTCUSDT" {
                            if let Some(price) = data["c"].as_str() {
                                if let Ok(price_float) = price.parse::<f64>() {
                                    data_points.push(price_float);
                                }
                            }
                        }
                    }
                }
            }
                Err(e) => eprintln!("Error receiving message: {:?}", e),
            }
        };

        let receive_task = tokio::spawn(async {
            let ws_stream = ws_stream;
            let handle_message = handle_message;
            async move {
                tokio_tungstenite::tungstenite::tokio::accept_async(ws_stream, |msg| {
                    handle_message(msg);
                    futures_util::future::ready(())
                })
                .await
                .expect("Error during WebSocket handshake");
            }
        });

        // Sleep for 1 second before closing the WebSocket
        sleep(Duration::from_secs(1)).await;

        // Cancel the WebSocket task
        receive_task.abort();
    }

    //receiving task for Q2
    let receive_task = tokio::spawn(async move {
        let ws_stream = ws_stream;
        let handle_message = handle_message;
        async move {
            tokio_tungstenite::tungstenite::tokio::accept_async(ws_stream, |msg| {
                handle_message(msg);
                futures_util::future::ready(())
            })
            .await.expect("Error during Websocket Handshake");
        }
    });

    // Sleep for 1 second before closing the Websocket
    sleep(Duration::from_secs(1)).await;

    // Cancel the websocket task
    receive_task.abort();

}


    if !data_points.is_empty() {
        let average_price = data_points.iter().sum::<f64>() / data_points.len() as f64;
        //sending average to aggreagator
        aggregator_tx.send(average_price).await.expect("error sending to aggreagtor");
        // let result = format!("Cache complete. The average USD price of BTC is: {}", average_price);

        let cache_data = CacheData {
            average_price,
            data_points: data_points.clone(),
        };

        let cache_data_json = serde_json::to_string(&cache_data).expect("Error serializing data");

        let mut file = File::create(output_file).expect("Error creating output file");
        file.write_all(cache_data_json.as_bytes())
            .expect("Error writing to file");
        
         // obtaining result for Q1
        // println!("{}", result);
    }



// getting Aggregator Average as stated in Q2
async fn aggreagator_mode(aggreagator_rx: tokio::sync::mpsc::Receiver<f64>) {
    let mut averages = Vec::with_capacity(NUM_CLIENTS);

    for _ in 0..NUM_CLIENTS {
        if let Some(average) = aggreagator_rx.recv().await {
            average.push(average);
        }
    }

    let final_average = averages.iter().sum::<f64>() / averages.len() as f64;

    let aggreagator_data = AggregatorData { averages };

    let aggregator_data_json =
        serde_json::to_string(&aggregator_data).expect("Error serializing aggregator data");

    println!(
        "Aggregator complete. The final average USD price of BTC is: {}",
        final_average
    );

    println!("Averages from individual clients: {:?}", averages);

    // Save aggregator data to a file
    let mut file = File::create("aggregator_output.json").expect("Error creating aggregator output file");
    file.write_all(aggregator_data_json.as_bytes())
        .expect("Error writing to file");

}

/**
//!SECTION readMode for Q1


async fn read_mode(input_file: &str) {
    let file = File::open(input_file).expect("Error opening input file");
    let cache_data: CacheData = serde_json::from_reader(file).expect("Error reading JSON from file");

    if !cache_data.data_points.is_empty() {
        let result = format!("Read complete. The average USD price of BTC is: {}", cache_data.average_price);

        println!("{}", result);
        println!("Data points used for aggregate: {:?}", cache_data.data_points);
    } else {
        println!("Invalid data format in the input file.");
    }
}
**/

/**!SECTION

//!SECTION Main Body for Q1

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 3 {
        println!("Usage: ./simple --mode=cache --times=10");
        println!("       ./simple --mode=read");
        std::process::exit(1);
    }

    let mode = &args[2];

    match mode.as_str() {
        "--mode=cache" => {
            let times_str = args.iter().find(|&arg| arg.starts_with("--times="));
            if let Some(times_str) = times_str {
                if let Ok(times) = times_str.replace("--times=", "").parse::<u64>() {
                    let output_file = "output.json";
                    cache_mode(times, output_file).await;
                } else {
                    println!("Invalid times argument.");
                    std::process::exit(1);
                }
            } else {
                println!("--times argument is missing.");
                std::process::exit(1);
            }
        }
        "--mode=read" => {
            let input_file = "output.json";
            read_mode(input_file).await;
        }
        _ => {
            println!("Invalid mode argument.");
            std::process::exit(1);
        }
    }
}
**/

fn main() {
    // fetching signiture and secret_key
    let secp = Arc::new(Mutex::new(Secp256k1::new()));
    let secret_key = SecretKey::from_slice(&[1; 32]).expect("secret key error");


    let start_time = chrono::Local::now().time().format("%H:%M:%S").to_string();
    println!("Starting all clients at: {}", start_time);

    let (aggregator_tx, aggregator_rx) = tokio::sync::mpsc::channel::<f64>(NUM_CLIENTS);

    // passing signiture to aggregator handle
    let aggregator_handle = tokio::spawn(aggregator_mode(aggregator_rx, Arc::clone(&secp)));

    let mut client_handles = Vec::with_capacity(NUM_CLIENTS);

    for _ in 0..NUM_CLIENTS {
        let aggregator_tx = aggregator_tx.clone();
        let handle = tokio::spawn(cache_mode(10, "output.json", aggregator_tx));
        client_handles.push(handle);
    }

    let _ = tokio::join!(aggregator_handle, async {
        for handle in client_handles {
            handle.await.expect("Error running client task");
        }
    });

    let end_time = chrono::Local::now().time().format("%H:%M:%S").to_string();
    println!("All clients finished at: {}", end_time);
}