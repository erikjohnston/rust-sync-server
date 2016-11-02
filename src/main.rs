#![feature(proc_macro)]

extern crate hyper;
extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
extern crate postgres;
#[macro_use]
extern crate quick_error;
extern crate indolentjson;
extern crate time;

use indolentjson::nodes::Value as IndolentValue;

mod types;

use types::*;

use postgres::{Connection, GenericConnection, TlsMode};
use postgres::error::Error as PostgresError;
use postgres::types::ToSql;

use serde_json::error::Error as JsonError;

use std::cmp::min;
use std::collections::{BTreeMap, BTreeSet};




quick_error! {
    #[derive(Debug)]
    pub enum DatabaseError {
        Postgres(err: PostgresError) {
            from()
            description("database error")
            display("Database error: {}", err)
            cause(err)
        }
        Json(err: JsonError) {
            from()
            description("json error")
            display("JSON error: {}", err)
            cause(err)
        }
    }
}


fn get_account_data_for_user<T: GenericConnection>(conn: &mut T, user_id: &str)
    -> Result<(Vec<AccountDataEvent>, BTreeMap<String, Vec<AccountDataEvent>>), DatabaseError>
{
    let global_sql = "SELECT account_data_type, content FROM account_data WHERE user_id = $1";

    let mut global = Vec::new();
    for row in &conn.query(global_sql, &[&user_id])? {
        let bytes: String = row.get(1);
        global.push(AccountDataEvent {
            etype: row.get(0),
            content: serde_json::from_str(&bytes[..])?,
        });
    }

    let mut by_room = BTreeMap::new();
    let room_sql = "SELECT room_id, account_data_type, content FROM room_account_data WHERE user_id = $1";
    for row in &conn.query(room_sql, &[&user_id])? {
        let bytes: String = row.get(2);
        by_room.entry(row.get(0)).or_insert_with(Vec::new).push(AccountDataEvent {
            etype: row.get(1),
            content: serde_json::from_str(&bytes[..])?,
        });
    }

    // TODO: Add push rules.

    Ok((global, by_room))
}


pub mod membership {
    pub const JOIN: &'static str = "join";
    pub const INVITE: &'static str = "invite";
    pub const LEAVE: &'static str = "leave";
    pub const BAN: &'static str = "ban";
}


fn get_rooms_for_user<T: GenericConnection>(
    conn: &mut T, user_id: &str, membership_list: &[&str]
) -> Result<BTreeMap<String, Vec<String>>, DatabaseError> {
    let sql = r#"
        SELECT membership, room_id
        FROM current_state_events as c
        INNER JOIN room_memberships AS m USING (room_id, event_id)
        INNER JOIN events AS e USING (room_id, event_id)
        WHERE user_id = $1 AND forgotten = 0 AND membership = ANY($2) AND c.state_key = m.user_id
    "#;

    let mut res: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for row in &conn.query(sql, &[&user_id, &membership_list])? {
        res.entry(row.get(0)).or_insert_with(Vec::new).push(row.get(1));
    }

    Ok(res)
}


fn get_room_events_stream_for_room<T: GenericConnection>(
    conn: &mut T, room_id: &str, top_stream_order: i64, filter: &FilterCollection,
) -> Result<(Vec<String>, i64, bool), DatabaseError> {
    let limit = filter.room.timeline.limit as i64;

    let query = if let Some(ref type_list) = filter.room.timeline.types {
        conn.query(r#"
            SELECT event_id, stream_ordering::bigint FROM events WHERE
            room_id = $1
            AND not outlier
            AND stream_ordering::bigint <= $2
            AND type = ANY($3)
            ORDER BY stream_ordering DESC LIMIT $4
        "#, &[&room_id, &top_stream_order, &type_list, &(limit + 1)])?
    } else {
        conn.query(r#"
            SELECT event_id, stream_ordering::bigint FROM events WHERE
            room_id = $1
            AND not outlier
            AND stream_ordering::bigint <= $2
            ORDER BY stream_ordering DESC LIMIT $3
        "#, &[&room_id, &top_stream_order, &(limit + 1)])?
    };

    let mut stream_ordering = top_stream_order;
    let mut event_ids = Vec::with_capacity(limit as usize + 1);
    for row in &query {
        event_ids.push(row.get(0));
        stream_ordering = min(stream_ordering, row.get(1));
    }

    let len = event_ids.len();
    let limited = if len == (limit as usize + 1) {
        event_ids.remove(limit as usize);
        true
    } else {
        false
    };

    Ok((event_ids, stream_ordering, limited))
}

fn get_filter<T: GenericConnection>(
    conn: &mut T, user_id: &str, filter_id: i64,
) -> Result<Option<FilterCollection>, DatabaseError> {
    let sql = "SELECT filter_json FROM user_filters WHERE user_id = $1 AND filter_id = $2";

    let localpart = &user_id.splitn(2, ":").next().unwrap()[1..];

    println!("Getting filter for: {} {}", localpart, filter_id);

    if let Some(row) = conn.query(sql, &[&localpart, &filter_id])?.iter().next() {
        let bytes: Vec<u8> = row.get(0);
        let filter: FilterCollection = serde_json::from_slice(&bytes[..])?;
        Ok(Some(filter))
    } else {
        Ok(None)
    }
}


fn get_max_stream_ordering<T: GenericConnection>(conn: &mut T) -> Result<i64, DatabaseError> {
    let sql = r#"SELECT coalesce(max(stream_ordering), 0)::bigint FROM events"#;

    Ok(conn.query(sql, &[])?.iter().map(|row| row.get(0)).max().unwrap_or(0))
}

fn get_current_receipts_token<T: GenericConnection>(conn: &mut T) -> Result<i64, DatabaseError> {
    let sql = r#"SELECT coalesce(max(stream_id), 0)::bigint FROM receipts_linearized"#;

    Ok(conn.query(sql, &[])?.iter().map(|row| row.get(0)).max().unwrap_or(0))
}


use std::borrow::Borrow;

fn get_event_jsons<T: GenericConnection, S: ToSql + Ord> (
    conn: &mut T, event_ids: &[S]
) -> Result<Vec<Event>, DatabaseError>
    where String: Borrow<S>
{
    let sql = r#"SELECT event_id, json FROM event_json WHERE event_id = any($1)"#;

    let mut event_map: BTreeMap<String, Event> = conn.query(sql, &[&event_ids])?
                    .iter()
                    .map(|row| (row.get(0), row.get(1)))
                    .map(|(e_id, bytes): (String, String)| {
                        let mut value = IndolentValue::from_slice(&bytes.as_bytes());
                        value.discard_key(b"auth_events");
                        value.discard_key(b"depth");
                        value.discard_key(b"hashes");
                        value.discard_key(b"origin");
                        value.discard_key(b"prev_events");
                        value.discard_key(b"prev_state");
                        value.discard_key(b"signatures");
                        Event {
                            event_id: e_id,
                            value: value,
                        }
                    })
                    .map(|e| (e.event_id.clone(), e))
                    .collect();

    Ok(event_ids.iter().filter_map(|e_id| event_map.remove(&e_id)).collect())
}

fn get_state_ids_for_event<T: GenericConnection>(
    conn: &mut T, event_id: &str, filter: &FilterCollection,
) -> Result<Vec<String>, DatabaseError> {
    let query = if let Some(ref type_list) = filter.room.state.types {
        conn.query(r#"
            WITH RECURSIVE state(state_group) AS (
                SELECT state_group FROM event_to_state_groups WHERE event_id = $1
                UNION ALL
                SELECT prev_state_group FROM state_group_edges e, state s
                WHERE s.state_group = e.state_group
            )
            SELECT DISTINCT last_value(event_id) OVER (
                PARTITION BY type, state_key ORDER BY state_group ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS event_id FROM state_groups_state
            WHERE state_group IN (
                SELECT state_group FROM state
            )
            AND type = ANY($2)
        "#, &[&event_id, type_list])?
    } else {
        conn.query(r#"
            WITH RECURSIVE state(state_group) AS (
                SELECT state_group FROM event_to_state_groups WHERE event_id = $1
                UNION ALL
                SELECT prev_state_group FROM state_group_edges e, state s
                WHERE s.state_group = e.state_group
            )
            SELECT DISTINCT last_value(event_id) OVER (
                PARTITION BY type, state_key ORDER BY state_group ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS event_id FROM state_groups_state
            WHERE state_group IN (
                SELECT state_group FROM state
            )
        "#, &[&event_id])?
    };

    let sg: Vec<String> = query.iter()
                               .map(|row| row.get(0))
                               .collect();

    Ok(sg)
}


fn get_state_for_room<T: GenericConnection>(
    conn: &mut T, room_id: &str, stream_ordering: i64, filter: &FilterCollection,
) -> Result<Vec<String>, DatabaseError> {
    let query = if let Some(ref type_list) = filter.room.state.types {
        conn.query(r#"
            WITH RECURSIVE state(state_group) AS (
                (
                    SELECT event_id FROM events WHERE stream_ordering <= $1 AND room_id = $2
                    ORDER BY stream_ordering DESC
                    LIMIT 1
                )
                UNION ALL
                SELECT prev_state_group FROM state_group_edges e, state s
                WHERE s.state_group = e.state_group
            )
            SELECT DISTINCT last_value(event_id) OVER (
                PARTITION BY type, state_key ORDER BY state_group ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS event_id FROM state_groups_state
            WHERE state_group IN (
                SELECT state_group FROM state
            )
            AND type = ANY($3)
        "#, &[&stream_ordering, &room_id, &type_list])?
    } else {
        conn.query(r#"
            WITH RECURSIVE state(state_group) AS (
                (
                    SELECT event_id FROM events WHERE stream_ordering <= $1 AND room_id = $2
                    ORDER BY stream_ordering DESC
                    LIMIT 1
                )
                UNION ALL
                SELECT prev_state_group FROM state_group_edges e, state s
                WHERE s.state_group = e.state_group
            )
            SELECT DISTINCT last_value(event_id) OVER (
                PARTITION BY type, state_key ORDER BY state_group ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS event_id FROM state_groups_state
            WHERE state_group IN (
                SELECT state_group FROM state
            )
        "#, &[&stream_ordering, &room_id])?
    };

    let sg: Vec<String> = query.iter()
                               .map(|row| row.get(0))
                               .collect();

    Ok(sg)
}


fn get_receipts_for_room<T: GenericConnection>(
    conn: &mut T, room_id: &str, current_token: i64,
) -> Result<ReceiptEvent, DatabaseError> {
    let sql = r#"
        SELECT receipt_type, event_id, user_id, data FROM receipts_linearized
        WHERE room_id = $1 AND stream_id <= $2
    "#;

    let mut content = BTreeMap::new();

    for row in &conn.query(sql, &[&room_id, &current_token])? {
        // {"$foo:bar": { "read": { "@user:host": <receipt> }, .. }, .. }
        let receipt_type = row.get(0);
        let event_id = row.get(1);
        let user_id = row.get(2);
        let bytes: String = row.get(3);
        let data = serde_json::from_str(&bytes[..])?;
        content.entry(event_id).or_insert_with(BTreeMap::new)
               .entry(receipt_type).or_insert_with(BTreeMap::new)
               .insert(user_id, data);
    }

    Ok(ReceiptEvent {
        etype: String::from("m.receipt"),
        content: content,
    })
}

fn get_unread_notifs_for_room<T: GenericConnection>(
    conn: &mut T, room_id: &str, user_id: &str,
) -> Result<UnreadNotification, DatabaseError> {
    let last_sql = r#"
        SELECT event_id FROM receipts_linearized
        WHERE receipt_type = 'm.read' AND room_id = $1 AND user_id = $2
        LIMIT 1
    "#;

    let last_event_id_opt: Option<String> = conn.query(last_sql, &[&room_id, &user_id])?.iter().next().map(|row| row.get(0));

    let (notify, highlight) = if let Some(last_event_id) = last_event_id_opt {
        let sql = r#"
            SELECT coalesce(sum(notif), 0)::bigint, coalesce(sum(highlight), 0)::bigint
            FROM events AS e, event_push_actions AS ea
            WHERE ea.room_id = $1 AND ea.user_id = $2 AND e.event_id = $3 AND
            (ea.topological_ordering, ea.stream_ordering) > (e.topological_ordering, e.stream_ordering)
        "#;

        let (notify, highlight) = conn.query(sql, &[&room_id, &user_id, &last_event_id])?.iter()
                                     .fold((0, 0), |acc, row| (acc.0 + row.get::<_, i64>(0), acc.1 + row.get::<_, i64>(1)));

        (notify, highlight)
    } else {
        (0, 0)
    };

    Ok(UnreadNotification {
        notification_count: notify as u64,
        highlight_count: highlight as u64,
    })
}


fn compute_state_delta<T: GenericConnection>(
    conn: &mut T, room_id: &str, event_ids: &[String], stream_ordering: i64, filter: &FilterCollection,
) -> Result<Vec<String>, DatabaseError> {
    // TODO: Assumes full_state

    if event_ids.len() > 0 {
        let current_state = get_state_ids_for_event(
            conn,
            &event_ids[event_ids.len()-1],
            filter,
        )?;

        let start_state = get_state_ids_for_event(
            conn,
            &event_ids[0],
            filter,
        )?;

        let event_ids_in_timeline: BTreeSet<&str> = event_ids.iter()
                                                               .map(String::as_ref)
                                                               .collect();

        let mut event_ids_to_include: Vec<String> = current_state.iter()
                                                           .chain(start_state.iter())
                                                           .map(String::as_ref)
                                                           .filter(|e_id| {
                                                               !event_ids_in_timeline.contains(e_id)
                                                           })
                                                           .map(str::to_owned)
                                                           .collect();

        event_ids_to_include.dedup();

        Ok(event_ids_to_include)
    } else {
        Ok(get_state_for_room(conn, room_id, stream_ordering, filter)?)
    }
}

fn get_user_by_access_token<T: GenericConnection>(
    conn: &mut T, access_token: &str,
) -> Result<Option<String>, DatabaseError> {
    let sql = "SELECT user_id FROM access_tokens WHERE token = $1";

    Ok(conn.query(sql, &[&access_token])?.iter().next().map(|row| row.get::<_, String>(0)))
}


fn generate_sync_response<T: GenericConnection>(
    conn: &mut T, user_id: &str, filter: FilterCollection,
)  -> Result<SyncResponse, DatabaseError> {

    let rooms_for_user = get_rooms_for_user(conn, user_id, &[
        membership::JOIN,
        membership::INVITE,
        membership::LEAVE,
        membership::BAN,
    ])?;

    println!("Got rooms for user");

    let empty_list = Vec::new();
    let joined_rooms = rooms_for_user.get(membership::JOIN).unwrap_or(&empty_list);

    let mut joined_responses = BTreeMap::new();

    let (_global_account_data, mut account_data_by_room) = get_account_data_for_user(conn, user_id)?;

    println!("Got account data");

    let current_receipts = get_current_receipts_token(conn)?;
    let current_stream_ordering = get_max_stream_ordering(conn)?;

    println!("Got current tokens");
    println!("Filter limit: {}", filter.room.timeline.limit);

    for room_id in joined_rooms {
        let (event_ids, prev_id, limited) = get_room_events_stream_for_room(
            conn, room_id, current_stream_ordering, &filter,
        )?;

        let state_delta = compute_state_delta(conn, room_id, &event_ids, current_stream_ordering, &filter)?;

        let prev_batch = format!("s{}_0_0_{}_0_0", prev_id, current_receipts);

        let mut batch = TimelineBatch {
            events: get_event_jsons(conn, &event_ids)?,
            prev_batch: prev_batch,
            limited: limited,
        };
        batch.events.reverse();

        let state = StateBatch {
            events: get_event_jsons(conn, &state_delta)?,
        };

        let receipt = get_receipts_for_room(conn, room_id, current_receipts)?;
        let mut ephemeral = Vec::new();
        if receipt.content.len() > 0 {
            ephemeral.push(receipt);
        }

        let joined = JoinedSyncResponse {
            timeline: batch,
            state: state,
            account_data: AccountData {
                events: account_data_by_room.remove(room_id).unwrap_or_default(),
            },
            ephemeral: Ephemeral {
                events: ephemeral,
            },
            unread_notifications: get_unread_notifs_for_room(
                conn, room_id, user_id
            )?
        };

        joined_responses.insert(room_id.clone(), joined);
    }

    let next_batch = format!("s{}_0_0_{}_0_0", current_stream_ordering, current_receipts);
    Ok(SyncResponse {
        rooms: RoomSyncResponse {
            join: joined_responses,
            invite: BTreeMap::new(),
            leave: BTreeMap::new(),
        },
        next_batch: next_batch,
        account_data: Vec::new(),
        presence: Presence {
            events: Vec::new(),
        },
    })
}


use hyper::Url;
use hyper::server::{Server, Request, Response};
use hyper::Client;
use hyper::status::StatusCode;

use std::io;

fn write_sync_response<W: io::Write>(writer: &mut W, sync: SyncResponse) -> serde_json::Result<()> {
    writer.write_all(b"{")?;

    write!(writer, r#""next_batch":"{}","#, &sync.next_batch)?;

    writer.write_all(br#""account_data":{},"#)?;

    writer.write_all(br#""presence":"#)?;
    serde_json::to_writer(writer, &sync.presence)?;
    writer.write_all(b",")?;

    writer.write_all(br#""rooms":{"#)?;

    writer.write_all(br#""invite":{},"#)?;
    writer.write_all(br#""leave":{},"#)?;

    writer.write_all(br#""join":{"#)?;

    let mut is_first = true;
    for (room_id, joined) in sync.rooms.join {
        if is_first {
            write!(writer, r#""{}":{{"#, &room_id)?;
            is_first = false;
        } else {
            write!(writer, r#","{}":{{"#, &room_id)?;
        }

        writer.write_all(br#""account_data":"#)?;
        serde_json::to_writer(writer, &joined.account_data)?;
        writer.write_all(b",")?;

        writer.write_all(br#""ephemeral":"#)?;
        serde_json::to_writer(writer, &joined.ephemeral)?;
        writer.write_all(b",")?;

        writer.write_all(br#""unread_notifications":"#)?;
        serde_json::to_writer(writer, &joined.unread_notifications)?;
        writer.write_all(b",")?;

        write!(
            writer,
            r#""timeline":{{"limited":{},"prev_batch":"{}","events":["#,
            joined.timeline.limited, &joined.timeline.prev_batch
        )?;
        let mut sub_is_first = true;
        for e in &joined.timeline.events {
            if !sub_is_first {
                writer.write_all(b",")?;
            } else {
                sub_is_first = false;
            }
            writer.write_all(e.value.as_bytes())?;
        }
        writer.write_all(b"]},")?;

        write!(
            writer,
            r#""state":{{"events":["#,
        )?;
        let mut sub_is_first = true;
        for e in &joined.state.events {
            if !sub_is_first {
                writer.write_all(b",")?;
            } else {
                sub_is_first = false;
            }
            writer.write_all(e.value.as_bytes())?;
        }
        writer.write_all(b"]}")?;

        writer.write_all(b"}")?;
    }

    writer.write_all(b"}}}")?;

    Ok(())
}

fn sync(mut req: Request, mut res: Response) {
    let client = Client::new();

    let request_path = match req.uri {
        hyper::uri::RequestUri::AbsolutePath(ref path) => {
            path.clone()
        },
        _ => {
            unimplemented!()
        }
    };

    println!("Got request: {}", request_path);

    let base_url = Url::parse("http://jki.re").unwrap();
    let new_url = base_url.join(&request_path).unwrap();

    let query_pairs: BTreeMap<_,_> = new_url.query_pairs().collect();

    let since_opt = query_pairs.get("since");

    if since_opt.is_some() || new_url.path() != "/_matrix/client/r0/sync" {
        println!("Proxying to: {}", new_url.as_str());

        let mut proxy_response = client.request(req.method.clone(), new_url.clone())
                                       .body(&mut req).send().unwrap();

        println!("Got response: {:?}", &proxy_response.status);

        *res.status_mut() = proxy_response.status.clone();
        *res.headers_mut() = proxy_response.headers.clone();
        let mut stream = res.start().unwrap();
        io::copy(&mut proxy_response, &mut stream).unwrap();
        stream.end().unwrap();

        return;
    }

    let ref access_token = query_pairs["access_token"];

    let mut conn = Connection::connect(
        "postgresql://erikj@%2Fvar%2Frun%2Fpostgresql/sytest1",
        TlsMode::None,
    ).unwrap();

    let user_id = get_user_by_access_token(&mut conn, access_token).unwrap().unwrap();

    println!("User: {}", &user_id);

    let filter = query_pairs.get("filter").map(|filter_bytes| {
        if let Some(filter_id) = filter_bytes.parse().ok() {
            get_filter(&mut conn, &user_id, filter_id).unwrap().unwrap()
        } else {
            serde_json::from_str(filter_bytes).unwrap()
        }
    }).unwrap_or_default();

    println!("Filter: {:#?}", &filter);

    *res.status_mut() = StatusCode::Ok;
    res.headers_mut().set_raw("Access-Control-Allow-Headers", vec![b"Origin, X-Requested-With, Content-Type, Accept".to_vec()]);
    res.headers_mut().set_raw("Access-Control-Allow-Origin", vec![b"*".to_vec()]);
    res.headers_mut().set_raw("Access-Control-Allow-Methods", vec![b"GET, POST, PUT, DELETE, OPTIONS".to_vec()]);
    res.headers_mut().set_raw("Content-Type", vec![b"application/json".to_vec()]);

    let start_gen = time::now();
    let json_resp = generate_sync_response(&mut conn, &user_id, filter).unwrap();
    let end_gen = time::now();

    let mut stream = res.start().unwrap();
    // serde_json::to_writer(&mut stream, &json_resp).unwrap();
    write_sync_response(&mut stream, json_resp).unwrap();
    stream.end().unwrap();

    let end_stream = time::now();

    println!(
        "Finished sync: {}ms, {}ms",
        (end_gen - start_gen).num_milliseconds(),
        (end_stream - end_gen).num_milliseconds(),
    );
}

use hyper::net::Openssl;

fn main() {
    // let ssl = Openssl::with_cert_and_key(
    //     "/home/erikj/git/synapse/demo/etc/localhost:8480.tls.crt",
    //     "/home/erikj/git/synapse/demo/etc/localhost:8480.tls.key",
    // ).unwrap();
    // Server::https("0.0.0.0:12345", ssl).unwrap().handle(sync).unwrap();
    Server::http("0.0.0.0:12345").unwrap().handle(sync).unwrap();
}
