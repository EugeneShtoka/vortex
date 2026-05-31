use anyhow::{Context, Result};
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use vortex_core::Event;

pub enum WsMsg {
    Connected,
    AppEvent(Event),
    Disconnected(Option<String>),
}

pub async fn connect(
    url: &str,
    token: &str,
    tx: mpsc::Sender<(usize, WsMsg)>,
    source_idx: usize,
) -> Result<()> {
    let mut req = url
        .into_client_request()
        .context("invalid WebSocket URL")?;
    req.headers_mut().insert(
        tokio_tungstenite::tungstenite::http::header::AUTHORIZATION,
        format!("Bearer {token}").parse()?,
    );

    let (mut ws, _) = tokio_tungstenite::connect_async(req)
        .await
        .context("WebSocket connection failed")?;

    let _ = tx.send((source_idx, WsMsg::Connected)).await;

    while let Some(msg) = ws.next().await {
        match msg {
            Ok(tokio_tungstenite::tungstenite::Message::Text(text)) => {
                if let Ok(event) = serde_json::from_str::<Event>(&text) {
                    if tx.send((source_idx, WsMsg::AppEvent(event))).await.is_err() {
                        break;
                    }
                }
            }
            Ok(tokio_tungstenite::tungstenite::Message::Close(_)) => break,
            Err(e) => {
                return Err(e.into());
            }
            _ => {}
        }
    }

    Ok(())
}
