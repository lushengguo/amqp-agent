use super::db::DB;
use super::models::Message;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::{info, warn};

pub struct MemoryCache {
    messages: HashMap<String, Message>,
    current_size: usize,
    max_size: usize,
    db: Arc<Mutex<DB>>,
}

impl MemoryCache {
    pub fn new(max_size: usize, db: Arc<Mutex<DB>>) -> Self {
        if max_size == 0 {
            panic!("Max size must be greater than 0");
        }

        Self {
            messages: HashMap::new(),
            current_size: 0,
            max_size,
            db,
        }
    }

    pub fn message_count(&self) -> u64 {
        self.messages.len() as u64
    }

    fn calculate_message_size(message: &Message) -> usize {
        message.url.len()
            + message.exchange.len()
            + message.exchange_type.len()
            + message.routing_key.len()
            + message.message.len()
            + std::mem::size_of::<u32>()
    }

    pub fn insert(&mut self, message: Message) {
        let locator = message.locator();
        let message_size = Self::calculate_message_size(&message);

        // 如果消息已存在，先移除旧消息
        if let Some(old_message) = self.messages.get(&locator) {
            self.current_size -= Self::calculate_message_size(old_message);
        }

        self.current_size += message_size;
        self.messages.insert(locator, message);

        while self.current_size >= self.max_size {
            self.flush_oldest_to_db();
        }
    }

    fn flush_oldest_to_db(&mut self) {
        let batch_size = self.max_size / 4;
        let mut to_remove = Vec::new();
        let mut to_db = Vec::new();

        // 按时间戳排序，将最旧的消息刷入数据库
        let mut messages: Vec<_> = self.messages.values().collect();
        messages.sort_by_key(|msg| msg.timestamp);

        for message in messages.iter().take(batch_size) {
            to_remove.push(message.locator());
            to_db.push((*message).clone());
        }

        if let Ok(mut db) = self.db.lock() {
            if let Err(e) = db.batch_insert(&to_db) {
                warn!("Failed to flush data to database: {}", e);
            } else {
                info!("Successfully flushed {} messages to database", to_db.len());

                for locator in to_remove {
                    if let Some(message) = self.messages.remove(&locator) {
                        self.current_size -= Self::calculate_message_size(&message);
                    }
                }
            }
        }
    }

    pub fn get_recent_messages(&self, n: usize) -> Vec<Message> {
        let mut result: Vec<_> = self.messages.values().cloned().collect();
        result.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        result.truncate(n);

        if result.len() < n {
            if let Ok(db) = self.db.lock() {
                if let Ok(db_messages) = db.get_recent_messages((n - result.len()) as u64) {
                    let cache_locators: std::collections::HashSet<_> =
                        result.iter().map(|msg| msg.locator()).collect();

                    let mut additional_messages: Vec<_> = db_messages
                        .into_iter()
                        .filter(|msg| !cache_locators.contains(&msg.locator()))
                        .collect();

                    result.append(&mut additional_messages);
                    result.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
                    result.truncate(n);
                }
            }
        }

        result
    }

    pub fn memory_usage(&self) -> usize {
        self.current_size
    }

    pub fn get_message_by_locator(&self, locator: &str) -> Option<Message> {
        self.messages.get(locator).cloned()
    }

    pub fn remove(&mut self, locator: &str) -> Option<Message> {
        let mut db = self.db.lock().unwrap();
        db.remove_message(&[locator.to_string()]).unwrap();

        if let Some(message) = self.messages.remove(locator) {
            self.current_size -= Self::calculate_message_size(&message);
            Some(message)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn create_test_message(
        exchange: &str,
        routing_key: &str,
        message_content: &str,
        timestamp_offset: u32,
    ) -> Message {
        let base_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as u32;

        Message {
            url: "amqp://localhost:5672".to_string(),
            exchange: exchange.to_string(),
            routing_key: routing_key.to_string(),
            message: message_content.to_string(),
            timestamp: base_timestamp + timestamp_offset,
            exchange_type: "topic".to_string(),
        }
    }

    fn setup_test_cache(max_size: usize) -> (MemoryCache, Arc<Mutex<DB>>, String) {
        let db_path = format!("test_cache_db_{}.sqlite", rand::random::<u32>());
        let db = DB::new_with_path(&db_path).unwrap();
        let db_arc = Arc::new(Mutex::new(db));
        let cache = MemoryCache::new(max_size, db_arc.clone());
        (cache, db_arc, db_path)
    }

    fn cleanup_test_resources(db_path: &str) {
        let _ = fs::remove_file(db_path);
    }

    #[test]
    fn test_cache_creation() {
        let (cache, _, db_path) = setup_test_cache(10);
        assert_eq!(cache.memory_usage(), 0);
        assert!(cache.messages.is_empty());
        cleanup_test_resources(&db_path);
    }

    #[test]
    fn test_insert_single_message() {
        let (mut cache, _, db_path) = setup_test_cache(1000);

        let message = create_test_message("test_exchange", "test_key", "Test message", 0);
        let locator = message.locator();
        let msg_size = MemoryCache::calculate_message_size(&message);
        cache.insert(message);

        assert_eq!(cache.memory_usage(), msg_size);
        assert!(cache.messages.contains_key(&locator));

        let recent_messages = cache.get_recent_messages(10);
        assert_eq!(recent_messages.len(), 1);
        assert_eq!(recent_messages[0].exchange, "test_exchange");
        assert_eq!(recent_messages[0].routing_key, "test_key");
        assert_eq!(recent_messages[0].message, "Test message");

        cleanup_test_resources(&db_path);
    }

    #[test]
    fn test_duplicate_message() {
        let (mut cache, _, db_path) = setup_test_cache(1000);

        let message = create_test_message("test_exchange", "test_key", "Test message", 0);
        let locator = message.locator();
        let msg_size = MemoryCache::calculate_message_size(&message);

        // 第一次插入
        cache.insert(message.clone());
        assert_eq!(cache.memory_usage(), msg_size);
        assert_eq!(cache.message_count(), 1);

        // 第二次插入相同消息
        cache.insert(message);
        assert_eq!(cache.memory_usage(), msg_size);
        assert_eq!(cache.message_count(), 1);

        // 验证消息内容
        let stored_message = cache.get_message_by_locator(&locator).unwrap();
        assert_eq!(stored_message.exchange, "test_exchange");
        assert_eq!(stored_message.routing_key, "test_key");
        assert_eq!(stored_message.message, "Test message");

        cleanup_test_resources(&db_path);
    }

    #[test]
    fn test_remove_message() {
        let (mut cache, _, db_path) = setup_test_cache(1000);

        let message = create_test_message("test_exchange", "test_key", "Test message", 0);
        let locator = message.locator();
        cache.insert(message);

        assert_eq!(cache.message_count(), 1);

        let removed = cache.remove(&locator);
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().routing_key, "test_key");
        assert_eq!(cache.message_count(), 0);
        assert_eq!(cache.memory_usage(), 0);

        cleanup_test_resources(&db_path);
    }

    #[test]
    fn test_cache_overflow() {
        let (mut cache, db_arc, db_path) = setup_test_cache(100);

        for i in 0..10 {
            let message = create_test_message(
                "test_exchange",
                &format!("key_{}", i),
                &format!("Message {}", i),
                i,
            );
            cache.insert(message);
        }

        // 验证部分消息已经被刷入数据库
        assert!(cache.memory_usage() <= cache.max_size);

        let db = db_arc.lock().unwrap();
        let db_messages = db.get_recent_messages(10).unwrap();
        assert!(!db_messages.is_empty());

        cleanup_test_resources(&db_path);
    }
}
