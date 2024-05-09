use amqprs::{
    callbacks::{DefaultChannelCallback, DefaultConnectionCallback},
    channel::{BasicPublishArguments, QueueBindArguments, QueueDeclareArguments},
    connection::{Connection, OpenConnectionArguments},
    BasicProperties,
};
use log::{info, trace};
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let connection = Connection::open(&OpenConnectionArguments::new(
        "geospatial-cluster.default.svc.cluster.local",
        5672,
        "default_user_X03UPcBDG-35mf0o58u",
        "6-gEf2FUCrbaQmHHo9tyuCknVvyh73FP",
    ))
    .await
    .unwrap();
    connection
        .register_callback(DefaultConnectionCallback)
        .await
        .unwrap();

    let channel = connection.open_channel(None).await.unwrap();
    channel
        .register_callback(DefaultChannelCallback)
        .await
        .unwrap();

    let (_, _, _) = channel
        .queue_declare(QueueDeclareArguments::new("opensky").finish())
        .await
        .unwrap()
        .unwrap();

    let mut conn_map: Box<HashMap<String, bool>> = Box::new(HashMap::new());

    loop {
        info!("Sending request to opensky...");
        let body = reqwest::get("https://opensky-network.org/api/states/all")
            .await?
            .text()
            .await?;
        info!("Got successful response");
        info!("Decoding as JSON...");
        let json: serde_json::Value = serde_json::from_str(body.as_str())?;
        info!("Successfully decoded");
        let states = &json["states"];
        trace!("{json}");
        let json_array = states
            .as_array()
            .ok_or("Returned data is not a JSON array")?;

        for record in json_array {
            trace!("{record}");

            let routing_key = record[0].as_str().unwrap().to_owned();

            let has_binding_channel = conn_map.entry(routing_key.clone()).or_default();
            let channel_clone = channel.clone();
            let routing_key_clone = routing_key.clone();
            if !*has_binding_channel {
                channel_clone
                    .queue_bind(QueueBindArguments::new(
                        "opensky",
                        "amq.topic",
                        routing_key_clone.as_str(),
                    ))
                    .await
                    .unwrap();
                conn_map.insert(routing_key.clone(), true);
            }
            let args = BasicPublishArguments::new("amq.topic", routing_key.as_str());
            let r_clone = record.clone();
            let channel_clone = channel.clone();
            tokio::task::spawn(async move {
                let mut header_table = amqprs::FieldTable::new();
                header_table.insert("icao24".try_into().unwrap(), routing_key.into());

                channel_clone
                    .basic_publish(
                        BasicProperties::default()
                            .with_headers(header_table)
                            .finish(),
                        serde_json::to_vec(&r_clone).unwrap(),
                        args.clone(),
                    )
                    .await
                    .unwrap();
            });
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
    }
}
