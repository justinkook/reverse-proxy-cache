mod cache;
mod error;
mod models;
mod settings;
mod storage;
mod task;
mod util;

use clap::{crate_version, App, Arg};
use regex::{Regex, RegexSet};
use settings::{Rule};
use task::TaskManager;
use tokio::sync::RwLock;

#[macro_use]
extern crate serde_derive;

extern crate pretty_env_logger;
#[macro_use]
extern crate log;

pub type LockedSharedTaskManager = RwLock<TaskManager>;

lazy_static::lazy_static! {
    /// A regular expression set of all specified rule paths and a list of Regex
    /// As suggest in regex documentation of `RegexSet`:
    /// Other features like finding the location of successive matches or their
    /// sub-captures aren’t supported. If you need this functionality, the
    /// recommended approach is to compile each regex in the set independently
    /// and selectively match them based on which regexes in the set matched.
    static ref RE_SET_LIST: RwLock<(RegexSet, Vec<Regex>)> = {
        RwLock::new((RegexSet::empty(), vec![]))
    };
    /// Global task manager.
    static ref TASK_MANAGER: LockedSharedTaskManager = {
        RwLock::new(TaskManager::empty())
    };
}

#[tokio::main]
async fn main() {
    let matches = App::new("mirror-cache")
        .version(crate_version!())
        .arg(
            Arg::with_name("config")
                .short("c")
                .long("config")
                .value_name("FILE")
                .help("Sets a custom config file. Default config.yml")
                .takes_value(true),
        )
        .get_matches();
    debug!("CLI args: {:?}", matches);
    let config_filename = matches
        .value_of("config")
        .unwrap_or("config.yml")
        .to_string();

    let app_settings = settings::Settings::new(&config_filename).unwrap();
    let port = app_settings.port;
    let api = filters::root();

    // initialize the logger
    let mut log_builder = pretty_env_logger::formatted_builder();
    log_builder
        .filter_module("hyper::proto", log::LevelFilter::Error) // hide excessive logs
        .filter_module("tracing::span", log::LevelFilter::Error)
        .filter_module("tokio_util::codec", log::LevelFilter::Error)
        .filter_level(app_settings.get_log_level())
        .init();

    // initialize global static TASK_MANAGER and RE_SET_LIST
    let mut tm = TaskManager::new(app_settings.clone());
    tm.refresh_config(&app_settings);
    {
        let mut global_tm = TASK_MANAGER.write().await;
        *global_tm = tm;
        let mut global_re_set_list = RE_SET_LIST.write().await;
        *global_re_set_list = create_re_set_list(&app_settings.rules);
    }

    warp::serve(api).run(([127, 0, 0, 1], port)).await;
}

fn create_re_set_list(rules: &[Rule]) -> (RegexSet, Vec<Regex>) {
    let rules_strings: Vec<String> = rules.iter().map(|rule| rule.path.clone()).collect();
    let set = RegexSet::new(&rules_strings).unwrap();
    let list = rules
        .iter()
        .map(|rule| Regex::new(&rule.path).unwrap())
        .collect();
    (set, list)
}

mod filters {
    use super::*;
    use warp::Filter;

    pub fn root() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        let log = warp::log::custom(|info| {
            info!(
                "🌐 {} {} Response: {}",
                info.method(),
                info.path(),
                info.status(),
            );
        });

        fallback_head().or(fallback().with(log))
    }

    fn fallback_head() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::head()
            .and(
                warp::path::tail().map(|tail: warp::filters::path::Tail| tail.as_str().to_string()),
            )
            .and_then(handlers::head_fallback_handler)
    }

    /// fallback handler, matches all paths
    fn fallback() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::get()
            .and(
                warp::path::tail().map(|tail: warp::filters::path::Tail| tail.as_str().to_string()),
            )
            .and_then(handlers::fallback_handler)
    }
}

mod handlers {
    use super::*;
    use crate::error::Error;
    use crate::task::Task;
    use std::result::Result;
    use warp::Rejection;
    use warp::Reply;

    pub async fn head_fallback_handler(path: String) -> Result<impl warp::Reply, Rejection> {
        // resolve path to upstream url
        let resolve_result = resolve_upstream(&path).await;
        if resolve_result.is_none() {
            return Err(warp::reject::not_found());
        }
        let upstream = resolve_result.unwrap().0;
        match util::make_request(&upstream, true).await {
            Ok(up_resp) => {
                // create a response and copy headers
                let resp_builder = up_resp
                    .headers()
                    .iter()
                    .fold(warp::http::Response::builder(), |prev, (key, value)| {
                        prev.header(key, value)
                    });
                Ok(resp_builder.body("").unwrap())
            }
            Err(e) => match e {
                Error::UpstreamRequestError(res) => {
                    let resp = warp::http::Response::builder()
                        .status(res.status())
                        .body("");
                    Ok(resp.unwrap())
                }
                _ => Err(warp::reject::custom(e)),
            },
        }
    }

    pub async fn fallback_handler(path: String) -> Result<impl warp::Reply, Rejection> {
        let upstream = resolve_upstream(&path).await;
        if upstream.is_none() {
            return Err(warp::reject());
        }
        let (upstream, idx, rule) = upstream.unwrap();
        trace!("matched by rule #{}: {}", idx, &rule.path);
        let task = Task {
            rule_id: idx,
            url: upstream,
        };
        let tm = TASK_MANAGER.read().await.clone();
        let tm_resp = tm.resolve_task(&task).await;
        match tm_resp.0 {
            Ok(data) => {
                let mut resp = data.into_response();
                if let Some(options) = &rule.options {
                    if let Some(content_type) = &options.content_type {
                        resp = warp::reply::with_header(resp, "content-type", content_type)
                            .into_response();
                    }
                }
                Ok(resp)
            }
            Err(e) => {
                match e {
                    Error::UpstreamRequestError(res) => {
                        let resp = warp::http::Response::builder()
                            .status(res.status())
                            .body(res.bytes().await.unwrap().into())
                            .unwrap();
                        Ok(resp)
                    }
                    _ => Err(warp::reject::custom(e)),
                }
            }
        }
    }

    /// Dynamically resolve upstream url as defined in config file
    async fn resolve_upstream(path: &str) -> Option<(String, usize, Rule)> {
        let tm = TASK_MANAGER.read().await.clone();
        let config = &tm.config;
        let rules_regex_set_list = RE_SET_LIST.read().await;
        let matched_indices: Vec<usize> =
            rules_regex_set_list.0.matches(path).into_iter().collect();
        if matched_indices.is_empty() {
            // No matching rule
            return None;
        }
        let idx = *matched_indices.first().unwrap();
        let re = &rules_regex_set_list.1[idx];
        let rule = config.rules.get(idx).unwrap();
        let upstream = rule.upstream.clone();
        trace!("matched by rule #{}: {}", idx, &rule.path);
        let replaced = re.replace_all(path, &upstream);
        Some((String::from(replaced), idx, rule.clone()))
    }
}
