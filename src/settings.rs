use crate::error::Error;
use crate::error::Result;
use config::{Config, Environment, File};

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    pub port: u16,
    redis: Redis,
    pub log_level: String,
    pub rules: Vec<Rule>,
    pub policies: Vec<Policy>,
    pub storages: Vec<Storage>,
}

#[derive(Debug, Deserialize, Clone)]
struct Redis {
    url: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Rule {
    pub name: Option<String>,
    pub path: String,
    pub policy: String,
    pub upstream: String,
    pub size_limit: Option<String>,
    pub rewrite: Option<Vec<Rewrite>>,
    pub options: Option<Options>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Policy {
    pub name: String,
    #[serde(rename = "type")]
    pub typ: PolicyType,
    pub metadata_db: MetadataDb,
    pub timeout: Option<u64>,
    pub size: Option<String>,
    pub storage: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Rewrite {
    pub from: String,
    pub to: String,
}

/// Options for rules
#[derive(Debug, Deserialize, Clone)]
pub struct Options {
    /// Override the content-type in the HTTP response header
    pub content_type: Option<String>,
}

#[derive(Debug, Deserialize, Copy, Clone)]
pub enum PolicyType {
    #[serde(rename = "TTL")]
    Ttl,
}

#[derive(Debug, Deserialize, Copy, Clone)]
pub enum MetadataDb {
    #[serde(rename = "redis")]
    Redis,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Storage {
    pub name: String,
    pub config: StorageConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub enum StorageConfig {
    Mem,
}

impl Settings {
    pub fn default() -> Self {
        Settings {
            port: 9000,
            redis: Redis {
                url: "redis://localhost".to_string(),
            },
            log_level: "info".to_string(),
            rules: vec![],
            policies: vec![],
            storages: vec![],
        }
    }

    pub fn new(filename: &str) -> Result<Self> {
        Self::new_from(filename, "app")
    }

    pub fn new_from(filename: &str, env_prefix: &str) -> Result<Self> {
        let mut s = Config::default();
        s.merge(File::with_name(filename))?;
        s.merge(Environment::with_prefix(env_prefix))?;
        match s.try_into() {
            Ok(settings) => {
                let mut settings: Settings = settings;
                // name all unnamed rules
                for (idx, rule) in settings.rules.iter_mut().enumerate() {
                    if rule.name.is_none() {
                        rule.name = Some(format!("rule_{}", idx));
                    }
                }
                Ok(settings)
            }
            Err(e) => Err(Error::ConfigDeserializeError(e)),
        }
    }

    pub fn get_redis_url(&self) -> String {
        self.redis.url.clone()
    }

    /// parse log level string to log::LevelFilter enum.
    /// The default log level is `info`.
    pub fn get_log_level(&self) -> log::LevelFilter {
        match self.log_level.to_lowercase().as_str() {
            "error" => log::LevelFilter::Error,
            "warn" => log::LevelFilter::Warn,
            "info" => log::LevelFilter::Info,
            "debug" => log::LevelFilter::Debug,
            "trace" => log::LevelFilter::Trace,
            _ => log::LevelFilter::Info,
        }
    }
}
