use postgres::{Connection, GenericConnection, SslMode};
use postgres::error::Error as PostgresError;
use postgres::types::ToSql;

use serde_json::error::Error as JsonError;

use std::cmp::min;
use std::collections::{BTreeMap, BTreeSet};

use serde_json;

use indolentjson::nodes::Value as IndolentValue;


#[derive(Debug, Clone)]
pub struct Event {
    pub event_id: String,
    pub value: IndolentValue,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AccountDataEvent {
    #[serde(rename="type")]
    pub etype: String,
    pub content: serde_json::Value,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ReceiptEvent {
    #[serde(rename="type")]
    pub etype: String,
    pub content: BTreeMap<String, BTreeMap<String, BTreeMap<String, serde_json::Value>>>,
}


#[derive(Debug, Clone)]
pub struct SyncResponse {
    pub next_batch: String,
    pub rooms: RoomSyncResponse,
    pub account_data: Vec<String>,
    pub presence: Presence,
}

#[derive(Debug, Clone)]
pub struct RoomSyncResponse {
    pub join: BTreeMap<String, JoinedSyncResponse>,
    pub invite: BTreeMap<String, String>,
    pub leave: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct JoinedSyncResponse {
    pub timeline: TimelineBatch,
    pub state: StateBatch,
    pub ephemeral: Ephemeral,
    pub account_data: AccountData,
    pub unread_notifications: UnreadNotification,
}

#[derive(Debug, Clone)]
pub struct StateBatch {
    pub events: Vec<Event>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AccountData {
    pub events: Vec<AccountDataEvent>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Ephemeral {
    pub events: Vec<ReceiptEvent>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Presence {
    pub events: Vec<String>,
}


#[derive(Debug, Clone)]
pub struct TimelineBatch {
    pub events: Vec<Event>,
    pub prev_batch: String,
    pub limited: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UnreadNotification {
    pub highlight_count: u64,
    pub notification_count: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct FilterCollection {
    #[serde(default)]
    pub room: FilterRoomCollection,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct FilterRoomCollection {
    #[serde(default)]
    pub timeline: Filter,
    #[serde(default)]
    pub state: Filter,
}

fn make_default_limit() -> u32 {
    10
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Filter {
    #[serde(default = "make_default_limit")]
    pub limit: u32,
    pub types: Option<Vec<String>>,
}

impl Default for Filter {
    fn default() -> Filter {
        Filter {
            limit: 10,
            types: None,
        }
    }
}
