use crate::error::Error;
use crate::error::Result;
use reqwest::ClientBuilder;

pub async fn make_request(url: &str, head: bool) -> Result<reqwest::Response> {
    let client = ClientBuilder::new().build().unwrap();
    let req = if !head {
        client.get(url)
    } else {
        client.head(url)
    };
    let resp = req.send().await;
    match resp {
        Ok(res) => {
            debug!("outbound request: {:?} {:?}", res.status(), res.headers());
            Ok(res)
        }
        Err(e) => {
            Err(Error::RequestError(e))
        }
    }
}

pub fn sleep_ms(ms: u64) {
    std::thread::sleep(std::time::Duration::from_millis(ms));
}
