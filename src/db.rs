use super::models::Message;
use rusqlite::{params, Connection, Result};
use std::fs;

pub struct DB {
    conn: Connection,
}

impl DB {
    pub fn new() -> Result<Self> {
        let conn = Connection::open("messages.db")?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS messages (
                locator TEXT PRIMARY KEY,
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

    pub fn message_count(&self) -> Result<u64> {
        let mut stmt = self.conn.prepare("SELECT COUNT(*) FROM messages")?;
        let count: u64 = stmt.query_row([], |row| row.get(0))?;
        Ok(count)
    }

    pub fn disk_usage(&self) -> Result<u64> {
        fs::metadata("messages.db")
            .map(|metadata| metadata.len())
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))
    }

    pub fn batch_insert(&mut self, messages: &[Message]) -> Result<()> {
        let tx = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT OR REPLACE INTO messages (locator, url, exchange, exchange_type, routing_key, message, timestamp) 
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            )?;

            for message in messages {
                stmt.execute(params![
                    message.locator(),
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

    pub fn remove_message(&mut self, locators: &[String]) -> Result<()> {
        let tx = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare("DELETE FROM messages WHERE locator = ?1")?;
            for locator in locators {
                stmt.execute(params![locator])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn get_message_by_locator(&self, locator: &str) -> Result<Option<Message>> {
        let mut stmt = self.conn.prepare(
            "SELECT url, exchange, routing_key, message, timestamp, exchange_type 
             FROM messages 
             WHERE locator = ?",
        )?;

        let mut messages = stmt
            .query_map(params![locator], |row| {
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

        Ok(messages.pop())
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
                locator TEXT PRIMARY KEY,
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
        let locator = message.locator();

        let result = db.batch_insert(&[message]);
        assert!(result.is_ok());

        let retrieved_message = db.get_message_by_locator(&locator).unwrap().unwrap();
        assert_eq!(retrieved_message.exchange, "test_exchange");
        assert_eq!(retrieved_message.routing_key, "test_key");
        assert_eq!(retrieved_message.message, "Hello, world!");

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

        let message_count = db.get_recent_messages(10).unwrap();
        assert_eq!(message_count.len(), 3);

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
    fn test_invalid_db_path() {
        let result = DB::new_with_path("/invalid/path/that/does/not/exist/db.sqlite");
        assert!(result.is_err());
    }

    #[test]
    fn test_remove_message() {
        let (mut db, db_path) = setup_test_db();

        let messages = vec![
            create_test_message("exchange1", "key1", "Message 1"),
            create_test_message("exchange2", "key1", "Message 2"),
        ];

        db.batch_insert(&messages).unwrap();

        let all_messages = db.get_recent_messages(10).unwrap();
        assert_eq!(all_messages.len(), 2);

        db.remove_message(&[messages[0].locator()]).unwrap();

        let remaining_messages = db.get_recent_messages(10).unwrap();
        assert_eq!(remaining_messages.len(), 1);
        assert_eq!(remaining_messages[0].message, "Message 2");

        cleanup_test_db(&db_path);
    }

    #[test]
    fn test_duplicate_message() {
        let (mut db, db_path) = setup_test_db();

        let message = create_test_message("exchange1", "key1", "Message 1");
        let locator = message.locator();

        // 第一次插入
        db.batch_insert(&[message.clone()]).unwrap();

        // 第二次插入相同的消息
        db.batch_insert(&[message.clone()]).unwrap();

        // 验证只存在一条记录
        let count = db.message_count().unwrap();
        assert_eq!(count, 1);

        // 验证消息内容正确
        let stored_message = db.get_message_by_locator(&locator).unwrap().unwrap();
        assert_eq!(stored_message.exchange, "exchange1");
        assert_eq!(stored_message.routing_key, "key1");
        assert_eq!(stored_message.message, "Message 1");

        cleanup_test_db(&db_path);
    }

    #[test]
    fn test_locator_uniqueness() {
        let (mut db, db_path) = setup_test_db();

        // 创建两条内容相同但时间戳不同的消息
        let message1 = create_test_message("exchange1", "key1", "Same content");
        std::thread::sleep(std::time::Duration::from_secs(1));
        let message2 = create_test_message("exchange1", "key1", "Same content");

        // 它们应该有相同的 locator
        assert_eq!(message1.locator(), message2.locator());

        // 插入第一条消息
        db.batch_insert(&[message1]).unwrap();
        let count1 = db.message_count().unwrap();
        assert_eq!(count1, 1);

        // 插入第二条消息（应该替换第一条）
        db.batch_insert(&[message2]).unwrap();
        let count2 = db.message_count().unwrap();
        assert_eq!(count2, 1);

        cleanup_test_db(&db_path);
    }

    #[test]
    fn test_get_nonexistent_message() {
        let (db, db_path) = setup_test_db();

        // 尝试获取不存在的消息
        let result = db.get_message_by_locator("nonexistent_locator").unwrap();
        assert!(result.is_none());

        cleanup_test_db(&db_path);
    }

    #[test]
    fn test_message_fields_affect_locator() {
        let base_message = create_test_message("exchange1", "key1", "Message 1");
        let base_locator = base_message.locator();

        // 测试不同字段的变化会产生不同的 locator
        let mut diff_url = base_message.clone();
        diff_url.url = "amqp://different:5672".to_string();
        assert_ne!(diff_url.locator(), base_locator);

        let mut diff_exchange = base_message.clone();
        diff_exchange.exchange = "different_exchange".to_string();
        assert_ne!(diff_exchange.locator(), base_locator);

        let mut diff_type = base_message.clone();
        diff_type.exchange_type = "fanout".to_string();
        assert_ne!(diff_type.locator(), base_locator);

        let mut diff_routing = base_message.clone();
        diff_routing.routing_key = "different_key".to_string();
        assert_ne!(diff_routing.locator(), base_locator);

        let mut diff_message = base_message.clone();
        diff_message.message = "Different message".to_string();
        assert_ne!(diff_message.locator(), base_locator);

        // 时间戳不应该影响 locator
        let mut diff_timestamp = base_message.clone();
        diff_timestamp.timestamp = 12345;
        assert_eq!(diff_timestamp.locator(), base_locator);
    }
}
