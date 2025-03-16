use super::db::DB;
use super::models::Message;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tracing::{info, warn};

pub struct MemoryCache {
    messages: BTreeMap<u32, Message>,
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
            messages: BTreeMap::new(),
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
        let timestamp = message.timestamp;
        self.current_size += Self::calculate_message_size(&message);
        self.messages.insert(timestamp, message);

        while self.current_size >= self.max_size {
            self.flush_oldest_to_db();
        }
    }

    fn flush_oldest_to_db(&mut self) {
        let batch_size = self.max_size / 4;
        let mut to_remove = Vec::new();
        let mut to_db = Vec::new();

        for (timestamp, message) in self.messages.iter() {
            if to_remove.len() >= batch_size {
                break;
            }
            to_remove.push(*timestamp);
            to_db.push(message.clone());
        }

        if let Ok(mut db) = self.db.lock() {
            if let Err(e) = db.batch_insert(&to_db) {
                warn!("Failed to flush data to database: {}", e);
            } else {
                info!("Successfully flushed {} messages to database", to_db.len());

                for timestamp in to_remove {
                    self.messages.remove(&timestamp);
                    self.current_size -= Self::calculate_message_size(&to_db[0]);
                }
            }
        }
    }

    pub fn get_recent_messages(&self, n: usize) -> Vec<Message> {
        let mut result = self
            .messages
            .iter()
            .rev()
            .take(n)
            .map(|(_, message)| message.clone())
            .collect::<Vec<Message>>();

        if result.len() < n {
            if let Ok(db) = self.db.lock() {
                if let Ok(db_messages) = db.get_recent_messages((n - result.len()) as u64) {
                    let cache_timestamps: std::collections::HashSet<_> =
                        result.iter().map(|msg| msg.timestamp).collect();

                    let mut additional_messages: Vec<_> = db_messages
                        .into_iter()
                        .filter(|msg| !cache_timestamps.contains(&msg.timestamp))
                        .collect();

                    result.append(&mut additional_messages);

                    result.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
                }
            }
        }

        result
    }

    pub fn memory_usage(&self) -> usize {
        self.current_size
    }

    pub fn remove(&mut self, timestamp: u32) -> Option<Message> {
        if let Some(message) = self.messages.remove(&timestamp) {
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
        let timestamp = message.timestamp;
        let msg_size = MemoryCache::calculate_message_size(&message);
        cache.insert(message);

        assert_eq!(cache.memory_usage(), msg_size);
        assert!(cache.messages.contains_key(&timestamp));

        let recent_messages = cache.get_recent_messages(10);
        assert_eq!(recent_messages.len(), 1);
        assert_eq!(recent_messages[0].exchange, "test_exchange");
        assert_eq!(recent_messages[0].routing_key, "test_key");
        assert_eq!(recent_messages[0].message, "Test message");

        cleanup_test_resources(&db_path);
    }

    #[test]
    fn test_cache_overflow() {
        let (mut cache, db_arc, db_path) = setup_test_cache(100);

        for i in 0..6 {
            let message = create_test_message(
                "test_exchange",
                &format!("key_{}", i),
                &format!(
                    "This is message number {} with some extra padding to make it larger",
                    i
                ),
                i,
            );
            let msg_size = MemoryCache::calculate_message_size(&message);
            cache.insert(message);
            println!("Inserted message size: {} bytes", msg_size);
        }

        assert!(cache.current_size <= cache.max_size);

        {
            let db = db_arc.lock().unwrap();
            let db_messages = db.get_recent_messages(10).unwrap();
            assert!(!db_messages.is_empty());
            assert!(db_messages.iter().any(|m| m.routing_key.contains("key_0")));
        }

        cleanup_test_resources(&db_path);
    }

    #[test]
    fn test_remove_message() {
        let (mut cache, _, db_path) = setup_test_cache(1000);

        let message = create_test_message("test_exchange", "test_key", "Test message", 0);
        let timestamp = message.timestamp;
        let msg_size = MemoryCache::calculate_message_size(&message);
        cache.insert(message);

        assert_eq!(cache.memory_usage(), msg_size);

        let removed = cache.remove(timestamp);
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().routing_key, "test_key");

        assert_eq!(cache.memory_usage(), 0);
        assert_eq!(cache.get_recent_messages(10).len(), 0);

        let not_found = cache.remove(12345);
        assert!(not_found.is_none());

        cleanup_test_resources(&db_path);
    }

    #[test]
    fn test_cache_ordering() {
        let (mut cache, _, db_path) = setup_test_cache(1000);

        let message1 = create_test_message("test_exchange", "key_1", "Message 1", 20);
        let message2 = create_test_message("test_exchange", "key_2", "Message 2", 10);
        let message3 = create_test_message("test_exchange", "key_3", "Message 3", 30);

        let total_size = MemoryCache::calculate_message_size(&message1)
            + MemoryCache::calculate_message_size(&message2)
            + MemoryCache::calculate_message_size(&message3);

        cache.insert(message1);
        cache.insert(message2);
        cache.insert(message3);

        assert_eq!(cache.memory_usage(), total_size);

        let messages = cache.get_recent_messages(10);
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0].routing_key, "key_3");
        assert_eq!(messages[1].routing_key, "key_1");
        assert_eq!(messages[2].routing_key, "key_2");

        cleanup_test_resources(&db_path);
    }

    #[test]
    fn test_performance_comparison() {
        let (mut cache, db_arc, db_path) = setup_test_cache(10000);

        let mut messages = Vec::new();
        for i in 0..1000 {
            messages.push(create_test_message(
                "perf_test",
                &format!("key_{}", i),
                &format!("Performance test message {}", i),
                i,
            ));
        }

        let warmup_message = create_test_message("warmup", "warmup", "warmup", 0);
        cache.insert(warmup_message.clone());
        {
            let mut db = db_arc.lock().unwrap();
            let _ = (*db).batch_insert(&vec![warmup_message]);
        }

        let start_cache = std::time::Instant::now();
        for message in messages.iter().take(100) {
            cache.insert(message.clone());
        }
        let cache_duration = start_cache.elapsed();

        cache.flush_oldest_to_db();

        let start_db = std::time::Instant::now();
        {
            let mut db = db_arc.lock().unwrap();
            let messages_slice = &messages[..100];
            (*db).batch_insert(messages_slice).unwrap();
        }
        let db_duration = start_db.elapsed();

        println!(
            "Time taken to cache 100 messages in memory: {:?}",
            cache_duration
        );
        println!(
            "Time taken to insert 100 messages directly to database: {:?}",
            db_duration
        );

        println!(
            "Cache/DB performance ratio: {:.2}",
            cache_duration.as_secs_f64() / db_duration.as_secs_f64()
        );

        cleanup_test_resources(&db_path);
    }

    #[test]
    fn test_cache_concurrent_access() {
        use std::thread;

        let (cache, _db_arc, db_path) = setup_test_cache(10000);
        let cache_arc = Arc::new(Mutex::new(cache));

        let threads: usize = 5;
        let messages_per_thread: usize = 20;
        let mut handles = Vec::new();

        for thread_id in 0..threads {
            let cache_clone = Arc::clone(&cache_arc);

            let handle = thread::spawn(move || {
                let mut messages = Vec::new();
                for i in 0..messages_per_thread {
                    messages.push(create_test_message(
                        &format!("thread_{}", thread_id),
                        &format!("key_{}", i),
                        &format!("Thread {} message {}", thread_id, i),
                        (thread_id * 100 + i) as u32,
                    ));
                }

                let mut cache = cache_clone.lock().unwrap();
                let mut thread_size = 0;
                for message in messages {
                    thread_size += MemoryCache::calculate_message_size(&message);
                    cache.insert(message);
                }
                thread_size
            });

            handles.push(handle);
        }

        let total_size: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();

        {
            let cache = cache_arc.lock().unwrap();
            assert_eq!(cache.memory_usage(), total_size);
        }

        cleanup_test_resources(&db_path);
    }

    #[test]
    fn test_automatic_flush_to_db() {
        let max_size = 400;
        let (mut cache, db_arc, db_path) = setup_test_cache(max_size);
        let mut total_size = 0;

        for i in 0..4 {
            let message = create_test_message(
                "test_exchange",
                &format!("key_{}", i),
                &format!(
                    "This is test message {} with additional content to make it larger",
                    i
                ),
                i,
            );
            let msg_size = MemoryCache::calculate_message_size(&message);
            total_size += msg_size;
            cache.insert(message);
            println!(
                "Inserted message size: {} bytes, total size: {} bytes",
                msg_size, total_size
            );
        }

        assert!(cache.current_size <= cache.max_size);

        {
            let db = db_arc.lock().unwrap();
            let db_messages = db.get_recent_messages(10).unwrap();
            if total_size > max_size {
                assert!(
                    !db_messages.is_empty(),
                    "Messages should be flushed to DB when size exceeds max_size"
                );
            } else {
                assert_eq!(
                    db_messages.len(),
                    0,
                    "No messages should be flushed when size is within limit"
                );
            }
        }

        let trigger_message = create_test_message(
            "test_exchange",
            "key_trigger",
            "This message should trigger a flush to database with extra content",
            100,
        );
        let trigger_size = MemoryCache::calculate_message_size(&trigger_message);
        println!("Trigger message size: {} bytes", trigger_size);
        cache.insert(trigger_message);

        assert!(cache.current_size <= cache.max_size);

        {
            let db = db_arc.lock().unwrap();
            let db_messages = db.get_recent_messages(10).unwrap();
            assert!(!db_messages.is_empty());
            assert!(db_messages.iter().any(|m| m.routing_key == "key_0"));
        }

        cleanup_test_resources(&db_path);
    }

    #[test]
    fn test_get_recent_with_db_fallback() {
        let (mut cache, db_arc, db_path) = setup_test_cache(1000);

        let cache_messages: Vec<_> = (0..3)
            .map(|i| {
                create_test_message(
                    "test_exchange",
                    &format!("cache_key_{}", i),
                    &format!("Cache message {}", i),
                    i,
                )
            })
            .collect();

        let db_messages: Vec<_> = (3..6)
            .map(|i| {
                create_test_message(
                    "test_exchange",
                    &format!("db_key_{}", i),
                    &format!("DB message {}", i),
                    i,
                )
            })
            .collect();

        for msg in cache_messages.iter() {
            cache.insert(msg.clone());
        }

        {
            let mut db = db_arc.lock().unwrap();
            db.batch_insert(&db_messages).unwrap();
        }

        let recent_messages = cache.get_recent_messages(5);

        assert_eq!(
            recent_messages.len(),
            5,
            "应该返回5条消息（缓存3条 + 数据库2条）"
        );

        assert!(
            recent_messages
                .windows(2)
                .all(|w| w[0].timestamp > w[1].timestamp),
            "消息应该按时间戳降序排序"
        );

        let cache_keys = recent_messages
            .iter()
            .filter(|m| m.routing_key.starts_with("cache_key"))
            .count();
        let db_keys = recent_messages
            .iter()
            .filter(|m| m.routing_key.starts_with("db_key"))
            .count();

        assert_eq!(cache_keys, 3, "应该有3条来自缓存的消息");
        assert_eq!(db_keys, 2, "应该有2条来自数据库的消息");

        cleanup_test_resources(&db_path);
    }

    #[test]
    fn test_get_recent_with_duplicate_prevention() {
        let (mut cache, db_arc, db_path) = setup_test_cache(1000);

        let test_message = create_test_message("test_exchange", "test_key", "Test message", 42);

        cache.insert(test_message.clone());
        {
            let mut db = db_arc.lock().unwrap();
            db.batch_insert(&vec![test_message.clone()]).unwrap();
        }

        let recent_messages = cache.get_recent_messages(2);

        assert_eq!(recent_messages.len(), 1, "相同的消息不应该重复返回");
        assert_eq!(
            recent_messages[0].timestamp, test_message.timestamp,
            "应该返回正确的消息"
        );

        cleanup_test_resources(&db_path);
    }
}
