use super::models::Message;
use rusqlite::{params, Connection, Result};

pub struct DB {
    conn: Connection,
}

impl DB {
    pub fn new() -> Result<Self> {
        let conn = Connection::open("messages.db")?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL,
                exchange TEXT NOT NULL,
                exchange_type TEXT NOT NULL,
                routing_key TEXT NOT NULL,
                message TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )",
            [],
        )?;
        Ok(Self { conn })
    }

    pub fn insert(&mut self, message: &Message) -> Result<()> {
        self.conn.execute(
            "INSERT INTO messages (url, exchange, exchange_type, routing_key, message, timestamp) 
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                message.url,
                message.exchange,
                message.exchange_type,
                message.routing_key,
                message.message,
                message.timestamp
            ],
        )?;
        Ok(())
    }

    pub fn batch_insert(&mut self, messages: &[Message]) -> Result<()> {
        let tx = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT INTO messages (url, exchange, exchange_type, routing_key, message, timestamp) 
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            )?;

            for message in messages {
                stmt.execute(params![
                    message.url,
                    message.exchange,
                    message.exchange_type,
                    message.routing_key,
                    message.message,
                    message.timestamp
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn get_recent_messages(&self, limit: u64) -> Result<Vec<Message>> {
        let mut stmt = self.conn.prepare(
            "SELECT url, exchange, routing_key, message, timestamp, exchange_type 
             FROM messages 
             ORDER BY timestamp DESC, created_at DESC 
             LIMIT ?",
        )?;

        let messages = stmt
            .query_map(params![limit], |row| {
                Ok(Message {
                    url: row.get(0)?,
                    exchange: row.get(1)?,
                    routing_key: row.get(2)?,
                    message: row.get(3)?,
                    timestamp: row.get(4)?,
                    exchange_type: row.get(5)?,
                })
            })?
            .collect::<Result<Vec<Message>>>()?;

        Ok(messages)
    }

    pub fn get_messages_by_exchange(&self, exchange: &str, limit: u64) -> Result<Vec<Message>> {
        let mut stmt = self.conn.prepare(
            "SELECT url, exchange, routing_key, message, timestamp, exchange_type 
             FROM messages 
             WHERE exchange = ?
             ORDER BY timestamp DESC, created_at DESC 
             LIMIT ?",
        )?;

        let messages = stmt
            .query_map(params![exchange, limit], |row| {
                Ok(Message {
                    url: row.get(0)?,
                    exchange: row.get(1)?,
                    routing_key: row.get(2)?,
                    message: row.get(3)?,
                    timestamp: row.get(4)?,
                    exchange_type: row.get(5)?,
                })
            })?
            .collect::<Result<Vec<Message>>>()?;

        Ok(messages)
    }

    #[cfg(test)]
    fn clear_all(&self) -> Result<()> {
        self.conn.execute("DELETE FROM messages", [])?;
        Ok(())
    }

    #[cfg(test)]
    pub fn new_with_path(path: &str) -> Result<Self> {
        let conn = Connection::open(path)?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL,
                exchange TEXT NOT NULL,
                exchange_type TEXT NOT NULL,
                routing_key TEXT NOT NULL,
                message TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )",
            [],
        )?;
        Ok(Self { conn })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn create_test_message(exchange: &str, routing_key: &str, message_content: &str) -> Message {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as u32;

        Message {
            url: "amqp://localhost:5672".to_string(),
            exchange: exchange.to_string(),
            exchange_type: "direct".to_string(),
            routing_key: routing_key.to_string(),
            message: message_content.to_string(),
            timestamp,
        }
    }

    fn setup_test_db() -> (DB, String) {
        let db_path = format!("test_db_{}.sqlite", rand::random::<u32>());
        let db = DB::new_with_path(&db_path).unwrap();
        (db, db_path)
    }

    fn cleanup_test_db(db_path: &str) {
        let _ = fs::remove_file(db_path);
    }

    #[test]
    fn test_db_creation() {
        let (db, db_path) = setup_test_db();

        assert!(fs::metadata(&db_path).is_ok());

        let table_exists = db
            .conn
            .query_row(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='messages'",
                [],
                |_| Ok(true),
            )
            .unwrap_or(false);

        assert!(table_exists, "messages table should exist");

        cleanup_test_db(&db_path);
    }

    #[test]
    fn test_insert_single_message() {
        let (mut db, db_path) = setup_test_db();

        let message = create_test_message("test_exchange", "test_key", "Hello, world!");

        let result = db.insert(&message);
        assert!(result.is_ok());

        let messages = db.get_recent_messages(10).unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].exchange, "test_exchange");
        assert_eq!(messages[0].routing_key, "test_key");
        assert_eq!(messages[0].message, "Hello, world!");

        cleanup_test_db(&db_path);
    }

    #[test]
    fn test_batch_insert() {
        let (mut db, db_path) = setup_test_db();

        let messages = vec![
            create_test_message("exchange1", "key1", "Message 1"),
            create_test_message("exchange1", "key2", "Message 2"),
            create_test_message("exchange2", "key1", "Message 3"),
        ];

        let result = db.batch_insert(&messages);
        assert!(result.is_ok());

        let stored_messages = db.get_recent_messages(10).unwrap();
        assert_eq!(stored_messages.len(), 3);

        cleanup_test_db(&db_path);
    }

    #[test]
    fn test_get_recent_messages_with_limit() {
        let (mut db, db_path) = setup_test_db();

        let mut messages = Vec::new();
        for i in 0..10 {
            messages.push(create_test_message(
                "test_exchange",
                &format!("key_{}", i),
                &format!("Message {}", i),
            ));

            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        db.batch_insert(&messages).unwrap();

        let recent_messages = db.get_recent_messages(5).unwrap();
        assert_eq!(recent_messages.len(), 5);

        for i in 0..recent_messages.len() - 1 {
            assert!(
                recent_messages[i].timestamp >= recent_messages[i + 1].timestamp,
                "messages are not sorted by timestamp"
            );
        }

        cleanup_test_db(&db_path);
    }

    #[test]
    fn test_get_messages_by_exchange() {
        let (mut db, db_path) = setup_test_db();

        let messages = vec![
            create_test_message("exchange1", "key1", "Message for exchange1 (1)"),
            create_test_message("exchange2", "key1", "Message for exchange2 (1)"),
            create_test_message("exchange1", "key2", "Message for exchange1 (2)"),
            create_test_message("exchange2", "key2", "Message for exchange2 (2)"),
            create_test_message("exchange3", "key1", "Message for exchange3"),
        ];

        db.batch_insert(&messages).unwrap();

        let exchange1_messages = db.get_messages_by_exchange("exchange1", 10).unwrap();
        assert_eq!(exchange1_messages.len(), 2);
        for msg in exchange1_messages {
            assert_eq!(msg.exchange, "exchange1");
        }

        let exchange2_messages = db.get_messages_by_exchange("exchange2", 10).unwrap();
        assert_eq!(exchange2_messages.len(), 2);
        for msg in exchange2_messages {
            assert_eq!(msg.exchange, "exchange2");
        }

        let exchange3_messages = db.get_messages_by_exchange("exchange3", 10).unwrap();
        assert_eq!(exchange3_messages.len(), 1);
        assert_eq!(exchange3_messages[0].exchange, "exchange3");

        let non_existent_messages = db.get_messages_by_exchange("non_existent", 10).unwrap();
        assert_eq!(non_existent_messages.len(), 0);

        cleanup_test_db(&db_path);
    }

    #[test]
    fn test_clear_all() {
        let (mut db, db_path) = setup_test_db();

        let messages = vec![
            create_test_message("exchange1", "key1", "Message 1"),
            create_test_message("exchange2", "key1", "Message 2"),
        ];

        db.batch_insert(&messages).unwrap();

        let all_messages = db.get_recent_messages(10).unwrap();
        assert_eq!(all_messages.len(), 2);

        db.clear_all().unwrap();

        let empty_messages = db.get_recent_messages(10).unwrap();
        assert_eq!(empty_messages.len(), 0);

        cleanup_test_db(&db_path);
    }

    #[test]
    fn test_transaction_rollback() {
        let (mut db, db_path) = setup_test_db();

        {
            let tx = db.conn.transaction().unwrap();
            tx.execute(
                "INSERT INTO messages (url, exchange, routing_key, message, timestamp) 
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    "amqp://localhost:5672",
                    "test_exchange",
                    "test_key",
                    "Transaction test message",
                    1000u32
                ],
            )
            .unwrap();
            tx.commit().unwrap();
        }

        let messages = db.get_recent_messages(10).unwrap();
        assert_eq!(messages.len(), 1);

        {
            let tx = db.conn.transaction().unwrap();
            tx.execute(
                "INSERT INTO messages (url, exchange, routing_key, message, timestamp) 
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    "amqp://localhost:5672",
                    "test_exchange",
                    "test_key",
                    "This message should not be committed",
                    2000u32
                ],
            )
            .unwrap();
        }

        let messages_after_rollback = db.get_recent_messages(10).unwrap();
        assert_eq!(messages_after_rollback.len(), 1);
        assert_eq!(messages_after_rollback[0].timestamp, 1000);

        cleanup_test_db(&db_path);
    }

    #[test]
    fn test_get_messages_by_exchange_with_limit() {
        let (mut db, db_path) = setup_test_db();

        let mut messages = Vec::new();
        for i in 0..20 {
            messages.push(create_test_message(
                "same_exchange",
                &format!("key_{}", i),
                &format!("Message {}", i),
            ));

            std::thread::sleep(std::time::Duration::from_millis(5));
        }

        db.batch_insert(&messages).unwrap();

        let limited_messages = db.get_messages_by_exchange("same_exchange", 5).unwrap();
        assert_eq!(limited_messages.len(), 5);

        let all_messages = db.get_messages_by_exchange("same_exchange", 100).unwrap();
        assert_eq!(all_messages.len(), 20);

        cleanup_test_db(&db_path);
    }

    #[test]
    fn test_invalid_db_path() {
        let result = DB::new_with_path("/invalid/path/that/does/not/exist/db.sqlite");
        assert!(result.is_err());
    }

    #[test]
    fn test_performance_batch_vs_individual() {
        let (mut db, db_path) = setup_test_db();

        let mut messages = Vec::new();
        for i in 0..100 {
            messages.push(create_test_message(
                "perf_test",
                &format!("key_{}", i),
                &format!("Performance test message {}", i),
            ));
        }

        let start_individual = std::time::Instant::now();
        for message in &messages {
            db.insert(message).unwrap();
        }
        let individual_duration = start_individual.elapsed();

        db.clear_all().unwrap();

        let start_batch = std::time::Instant::now();
        db.batch_insert(&messages).unwrap();
        let batch_duration = start_batch.elapsed();

        let all_messages = db.get_recent_messages(200).unwrap();
        assert_eq!(all_messages.len(), 100);

        println!("Single insert duration: {:?}", individual_duration);
        println!("Batch insert duration: {:?}", batch_duration);
        println!(
            "Batch insert performance improvement: {:.2}x",
            individual_duration.as_micros() as f64 / batch_duration.as_micros() as f64
        );

        assert!(batch_duration < individual_duration);

        cleanup_test_db(&db_path);
    }

    #[test]
    fn test_concurrent_access() {
        use std::sync::{Arc, Mutex};
        use std::thread;

        let (db, db_path) = setup_test_db();
        let db = Arc::new(Mutex::new(db));

        let mut handles = vec![];

        for thread_id in 0..5 {
            let db_clone = Arc::clone(&db);
            let handle = thread::spawn(move || {
                let mut messages = Vec::new();
                for i in 0..20 {
                    messages.push(create_test_message(
                        &format!("thread_{}", thread_id),
                        &format!("key_{}", i),
                        &format!("Message from thread {} - {}", thread_id, i),
                    ));
                }

                let mut db = db_clone.lock().unwrap();
                db.batch_insert(&messages).unwrap();
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        {
            let db_ref = db.lock().unwrap();
            let all_messages = db_ref.get_recent_messages(200).unwrap();
            assert_eq!(all_messages.len(), 100);

            for thread_id in 0..5 {
                let thread_messages = db_ref
                    .get_messages_by_exchange(&format!("thread_{}", thread_id), 100)
                    .unwrap();
                assert_eq!(thread_messages.len(), 20);
            }
        }

        cleanup_test_db(&db_path);
    }
}
