// Copyright (c) Silence Laboratories Pte. Ltd. All Rights Reserved.
// This software is licensed under the Silence Laboratories License Agreement.

use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    collections::HashMap,
};

use futures_util::{Sink, SinkExt, Stream};
use log::{debug, error, info};
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, QoS};
use tokio::sync::mpsc;

use sl_mpc_mate::coord::{MaybeFeed, MessageRelayService, MessageSendError, Relay};
use sl_mpc_mate::message::{AskMsg, MsgHdr, MsgId, MESSAGE_HEADER_SIZE};

/// MQTT-based message relay implementation
pub struct MqttMessageRelay {
    rx: mpsc::Receiver<Vec<u8>>,
    client: AsyncClient,
    client_id: String,
    topic_prefix: String,
}

impl Stream for MqttMessageRelay {
    type Item = Vec<u8>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

impl Sink<Vec<u8>> for MqttMessageRelay {
    type Error = MessageSendError;

    fn poll_ready(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(
        self: Pin<&mut Self>,
        item: Vec<u8>,
    ) -> Result<(), Self::Error> {
        let this = self.get_mut();
        let client_id = this.client_id.clone();
        // 直接发布到MQTT，不进行复杂的消息存储
        if let Ok(hdr) = item.as_slice().try_into() {
            let hdr: &MsgHdr = hdr;

            if item.len() > MESSAGE_HEADER_SIZE {
                // PUB消息 - 发布到MQTT
                let topic = format!("{}/messages/{:?}", this.topic_prefix, hdr.id());
                let client = this.client.clone();
                let message = item.clone();

                tokio::spawn(async move {
                    if let Err(e) = client.publish(&topic, QoS::ExactlyOnce, true, message).await {
                        println!("client_id {} Failed to publish message to MQTT: {}",  client_id, e);
                    } else {
                        println!("client_id {}  Message published to topic: {}", client_id, topic);
                    }
                });
            }
            // ASK消息不需要特殊处理，因为订阅已经在ask方法中完成
        }

        Ok(())
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

impl Relay for MqttMessageRelay {
    fn ask(&mut self, id: &MsgId, ttl: u32) -> MaybeFeed<'_, Self> {
        // 订阅对应的topic
        let topic = format!("{}/messages/{:?}", self.topic_prefix, id);
        let client = self.client.clone();

        info!("Subscribing to topic: {}", topic);

        tokio::spawn(async move {
            if let Err(e) = client.subscribe(&topic, QoS::ExactlyOnce).await {
                error!("Failed to subscribe to topic {}: {}", topic, e);
            } else {
                debug!("Successfully subscribed to topic: {}", topic);
            }
        });

        // 创建ASK消息
        let ask_msg = AskMsg::allocate(id, ttl);
        MaybeFeed::new(self.feed(ask_msg))
    }
}

#[derive(Debug)]
pub struct MqttMessageRelayService {
    mqtt_options: MqttOptions,
    topic_prefix: String,
}

impl MqttMessageRelayService {
    pub fn new<S: Into<String>>(broker: S, port: u16, client_id: S, topic_prefix: S) -> Self {
        let mut mqtt_options = MqttOptions::new(client_id.into(), broker.into(), port);
        mqtt_options.set_keep_alive(std::time::Duration::from_secs(30))
            .set_max_packet_size(10 * 1024 * 1024, 10 * 1024 * 1024)
            .set_clean_session(true);

        Self {
            mqtt_options,
            topic_prefix: topic_prefix.into(),
        }
    }

    pub fn client_id(&self) -> String {
        self.mqtt_options.client_id()
    }
    pub async fn connect(&self) -> Result<MqttMessageRelay, Box<dyn std::error::Error + Send + Sync>> {
        let client_id = self.client_id();
        let client_id_clone = client_id.clone();
        let (client, mut eventloop) = AsyncClient::new(self.mqtt_options.clone(), 1000);
        let (tx, rx) = mpsc::channel(1000);

        let topic_prefix = self.topic_prefix.clone();

        // 监听所有消息的通配符订阅
        let subscribe_topic = format!("{}/messages/+", topic_prefix);
        client.subscribe(&subscribe_topic, QoS::ExactlyOnce).await?;
        info!("Subscribed to MQTT topic: {}", subscribe_topic);

        // 处理收到的MQTT消息
        tokio::spawn(async move {
            loop {
                match eventloop.poll().await {
                    Ok(Event::Incoming(Incoming::Publish(publish))) => {
                        println!("client_id {} Received MQTT message on topic: {}", client_id, publish.topic);
                        if let Err(e) = tx.send(publish.payload.to_vec()).await {
                            println!("Failed to forward message: {}", e);
                            // 不要break，继续处理
                            continue;
                        }
                    }
                    Ok(Event::Incoming(Incoming::ConnAck(ack))) => {
                        info!("MQTT connection acknowledged: {:?}", ack);
                    }
                    Err(e) => {
                        println!("MQTT event loop error: {}", e);
                        // 等待一段时间后继续，而不是退出
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                    _ => {}
                }
            }
        });

        Ok(MqttMessageRelay {
            rx,
            client,
            client_id: client_id_clone,
            topic_prefix: self.topic_prefix.clone(),
        })
    }
}

impl MessageRelayService for MqttMessageRelayService {
    type MessageRelay = MqttMessageRelay;

    async fn connect(&self) -> Option<Self::MessageRelay> {
        match self.connect().await {
            Ok(relay) => Some(relay),
            Err(e) => {
                error!("Failed to connect to MQTT broker: {}", e);
                None
            }
        }
    }
}