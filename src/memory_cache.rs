use super::db::DB;
use super::models::Message;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tracing::{error, info, warn};

const LOCK_TIMEOUT: Duration = Duration::from_secs(10);

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

        let mut messages: Vec<_> = self.messages.values().collect();
        messages.sort_by_key(|msg| msg.timestamp);

        for message in messages.iter().take(batch_size) {
            to_remove.push(message.locator());
            to_db.push((*message).clone());
        }

        match self.db.try_lock_for(LOCK_TIMEOUT) {
            Some(mut db) => {
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
            None => {
                let bt = std::backtrace::Backtrace::capture();
                error!("Deadlock detected in flush_oldest_to_db: {:?}", bt);
                panic!("Deadlock detected while trying to acquire database lock");
            }
        }
    }

    pub fn get_recent_messages(&self, n: usize) -> Vec<Message> {
        let mut result: Vec<_> = self.messages.values().cloned().collect();
        result.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        result.truncate(n);

        if result.len() < n {
            match self.db.try_lock_for(LOCK_TIMEOUT) {
                Some(db) => {
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
                None => {
                    let bt = std::backtrace::Backtrace::capture();
                    error!("Deadlock detected in get_recent_messages: {:?}", bt);
                    panic!("Deadlock detected while trying to acquire database lock");
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
        match self.db.try_lock_for(LOCK_TIMEOUT) {
            Some(mut db) => {
                db.remove_message(&[locator.to_string()]).unwrap();
                if let Some(message) = self.messages.remove(locator) {
                    self.current_size -= Self::calculate_message_size(&message);
                    Some(message)
                } else {
                    None
                }
            }
            None => {
                let bt = std::backtrace::Backtrace::capture();
                error!("Deadlock detected in remove: {:?}", bt);
                panic!("Deadlock detected while trying to acquire database lock");
            }
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

        cache.insert(message.clone());
        assert_eq!(cache.memory_usage(), msg_size);
        assert_eq!(cache.message_count(), 1);

        cache.insert(message);
        assert_eq!(cache.memory_usage(), msg_size);
        assert_eq!(cache.message_count(), 1);

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

        assert!(cache.memory_usage() <= cache.max_size);

        let db = db_arc.lock();
        let db_messages = db.get_recent_messages(10).unwrap();
        assert!(!db_messages.is_empty());

        cleanup_test_resources(&db_path);
    }
}
