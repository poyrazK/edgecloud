//! `edge:http-client` — outbound HTTP requests.

pub struct HttpClient {
    client: reqwest::blocking::Client,
}

impl Default for HttpClient {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpClient {
    pub fn new() -> Self {
        let client = reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .expect("reqwest client creation failed");
        Self { client }
    }

    pub fn fetch(
        &self,
        method: &str,
        url: &str,
        headers: &[(String, String)],
        body: Option<&[u8]>,
    ) -> Result<HttpResponse, String> {
        let mut req = self.client.request(
            reqwest::Method::from_bytes(method.as_bytes())
                .map_err(|e| format!("invalid method: {}", e))?,
            url,
        );

        for (k, v) in headers {
            req = req.header(k, v);
        }

        if let Some(b) = body {
            req = req.body(b.to_vec());
        }

        let response = req.send().map_err(|e| format!("request failed: {}", e))?;

        let status = response.status().as_u16();
        let headers: std::collections::HashMap<_, _> = response
            .headers()
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();
        let body = response
            .bytes()
            .map_err(|e| format!("failed to read body: {}", e))?;

        Ok(HttpResponse {
            status,
            headers,
            body: body.to_vec(),
        })
    }
}

#[derive(serde::Serialize)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: std::collections::HashMap<String, String>,
    pub body: Vec<u8>,
}

