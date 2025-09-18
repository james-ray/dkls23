// examples/sign.rs
use k256::ecdsa::{RecoveryId, VerifyingKey};
use rand::Rng;
use rand_chacha::ChaCha20Rng;
use rand_core::SeedableRng;
use sl_dkls23::sign;
use tokio::task::JoinSet;

// 使用新的MqttMessageRelayService
use msg_relay::MqttMessageRelayService;

mod common;

#[tokio::main]
async fn main() {
    // 我们本地生成一些密钥共享来测试签名过程
    let shares = common::shared::gen_keyshares(2, 3).await;

    // 从其中一个密钥共享获取公共验证密钥
    let vk = VerifyingKey::from_affine(shares[0].public_key().to_affine())
        .unwrap();

    // 定义签名的链路径：m 是默认路径
    let chain_path = "m";

    // 在这里，参与方被模拟为真实世界的例子，但在本地作为一组 Rust 异步任务运行：
    // 每个节点一个任务来运行 dkls23 ecdsa 签名算法
    let mut parties = JoinSet::new();

    println!("Connecting to MQTT broker for signing...");

    for (i, setup) in common::shared::setup_dsg(&shares[0..2], chain_path).into_iter().enumerate() {
        let mut rng = ChaCha20Rng::from_entropy();

        // 为每个参与方创建独立的MQTT服务实例
        let client_id = format!("dkls23-sign-party-{}", i);
        let mqtt_service = MqttMessageRelayService::new(
            "localhost",
            1883,
            &client_id,
            "dkls23"
        );

        println!("Signing party {} connecting to MQTT with client_id: {}", i, mqtt_service.client_id());

        parties.spawn(async move {
            match mqtt_service.connect().await {
                Ok(relay) => {
                    println!("Signing party {} connected successfully", i);
                    match sign::run(setup, rng.gen(), relay).await {
                        Ok(result) => {
                            println!("Signing party {} completed successfully", i);
                            Ok(result)
                        },
                        Err(err) => {
                            eprintln!("Signing party {} failed with error: {:?}", i, err);
                            Err(err)
                        }
                    }
                }
                Err(err) => {
                    eprintln!("Signing party {} failed to connect to MQTT broker, err {}", i, err);
                    Err(sl_dkls23::sign::types::SignError::AbortProtocol(2))
                }
            }
        });
    }

    let mut successful_signatures = 0;

    // 在所有任务完成后，我们提取签名并针对公钥进行验证
    while let Some(result) = parties.join_next().await {
        match result {
            Ok(Ok((sign, recid))) => {
                successful_signatures += 1;

                let hash = [1u8; 32];
                let recid2 = RecoveryId::trial_recovery_from_prehash(&vk, &hash, &sign)
                    .unwrap();

                assert_eq!(recid, recid2);
                println!("✅ Signature {} verified successfully against public key", successful_signatures);
            }
            Ok(Err(err)) => {
                eprintln!("Signing party failed with error: {:?}", err);
            }
            Err(join_err) => {
                eprintln!("Signing party task panicked: {:?}", join_err);
            }
        }
    }

    if successful_signatures > 0 {
        println!("\n✅ Signing successful! {} signatures generated and verified.", successful_signatures);
    } else {
        println!("\n❌ Signing failed. No valid signatures produced.");
    }
}