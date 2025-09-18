// examples/keygen.rs
use k256::elliptic_curve::group::GroupEncoding;
use rand::Rng;
use rand_chacha::ChaCha20Rng;
use rand_core::SeedableRng;
use sl_dkls23::keygen::run as keygen_run;
use std::sync::Arc;

// 使用新的MqttMessageRelayService
use msg_relay::MqttMessageRelayService;

mod common;

#[tokio::main]
pub async fn main() {
    let t: u8 = 2;
    let n: u8 = 3;

    println!("Connecting to MQTT broker...");

    let mut parties = tokio::task::JoinSet::new();

    for (i, setup) in common::shared::setup_keygen(t, n, None).into_iter().enumerate() {
        parties.spawn({
            async move {
                // 为每个参与方创建独立的MQTT服务实例
                let client_id = format!("dkls23-keygen-party-{}", i);
                let mqtt_service = MqttMessageRelayService::new(
                    "localhost",
                    1883,
                    &client_id,  // 添加 & 引用
                    "dkls23"
                );

                println!("Party {} connecting to MQTT with client_id: {}", i, mqtt_service.client_id());  // 使用访问方法

                match mqtt_service.connect().await {
                    Ok(relay) => {  // 使用 Ok
                        println!("Party {} connected successfully", i);
                        let mut rng = ChaCha20Rng::from_entropy();
                        match keygen_run(setup, rng.gen(), relay).await{
                            Ok(share) => {
                                println!("Party {} completed key generation", i);
                                Ok(share)
                            },
                            Err(err) => {
                                println!("Party {} key generation failed, err {:?}", i, err);
                                Err(err)
                            }
                        }
                    }
                    Err(err) => {  // 使用 Err
                        eprintln!("Party {} failed to connect to MQTT broker, err {}", i, err);
                        Err(sl_dkls23::keygen::types::KeygenError::AbortProtocol(2))
                    }
                }
            }
        });
    }

    let mut shares = vec![];
    let mut successful_parties = 0;

    while let Some(result) = parties.join_next().await {
        match result {
            Ok(Ok(share)) => {
                shares.push(Arc::new(share));
                successful_parties += 1;
                println!("Party completed successfully (total: {})", successful_parties);
            }
            Ok(Err(err)) => {
                eprintln!("Party failed with error: {:?}", err);
            }
            Err(join_err) => {
                eprintln!("Party task panicked: {:?}", join_err);
            }
        }
    }

    // 打印公钥
    println!("\nGenerated public keys:");
    for (i, keyshare) in shares.iter().enumerate() {
        let pk_bytes = keyshare.public_key().to_bytes();
        println!("Party {} PK: {} si {:?}", i, hex::encode(&pk_bytes), keyshare.s_i());

        // 验证所有公钥是否相同
        if i > 0 {
            let first_pk = shares[0].public_key().to_bytes();
            if pk_bytes != first_pk {
                eprintln!("ERROR: Public key mismatch between party 0 and party {}", i);
            }
        }
    }

    if successful_parties >= t as usize {
        println!("\n✅ Key generation successful! {} parties completed.", successful_parties);
    } else {
        println!("\n❌ Key generation failed. Only {} parties completed, need at least {}.", successful_parties, t);
    }
}