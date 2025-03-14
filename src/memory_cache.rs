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
        Self {
            messages: BTreeMap::new(),
            current_size: 0,
            max_size,
            db,
        }
    }

    pub fn insert(&mut self, message: Message) {
        if self.current_size >= self.max_size {
            self.flush_oldest_to_db();
        }

        let timestamp = message.timestamp;
        self.messages.insert(timestamp, message);
        self.current_size += 1;
    }

    pub fn batch_insert(&mut self, messages: Vec<Message>) {
        for message in messages {
            self.insert(message);
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
            match db.batch_insert(&to_db) {
                Ok(_) => {
                    info!("成功将 {} 条消息刷新到数据库", to_db.len());

                    for timestamp in to_remove {
                        self.messages.remove(&timestamp);
                        self.current_size -= 1;
                    }
                }
                Err(e) => {
                    warn!("刷新数据到数据库失败: {}", e);
                }
            }
        }
    }

    pub fn get_recent(&self, n: usize) -> Vec<Message> {
        self.messages
            .iter()
            .rev()
            .take(n)
            .map(|(_, message)| message.clone())
            .collect()
    }

    pub fn get_by_exchange(&self, exchange: &str, n: usize) -> Vec<Message> {
        self.messages
            .iter()
            .rev()
            .filter(|(_, msg)| msg.exchange == exchange)
            .take(n)
            .map(|(_, message)| message.clone())
            .collect()
    }

    pub fn flush_all_to_db(&mut self) {
        if let Ok(mut db) = self.db.lock() {
            let messages: Vec<Message> = self.messages.values().cloned().collect();
            match db.batch_insert(&messages) {
                Ok(_) => {
                    info!("成功将所有消息({})刷新到数据库", messages.len());
                    self.messages.clear();
                    self.current_size = 0;
                }
                Err(e) => {
                    warn!("刷新所有数据到数据库失败: {}", e);
                }
            }
        }
    }

    pub fn size(&self) -> usize {
        self.current_size
    }

    pub fn max_size(&self) -> usize {
        self.max_size
    }

    pub fn remove(&mut self, timestamp: u32) -> Option<Message> {
        if let Some(message) = self.messages.remove(&timestamp) {
            self.current_size -= 1;
            Some(message)
        } else {
            None
        }
    }

    pub fn remove_batch(&mut self, timestamps: &[u32]) -> Vec<Message> {
        let mut removed = Vec::new();
        for timestamp in timestamps {
            if let Some(message) = self.remove(*timestamp) {
                removed.push(message);
            }
        }
        removed
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

        assert_eq!(cache.size(), 0);
        assert_eq!(cache.max_size(), 10);
        assert!(cache.messages.is_empty());

        cleanup_test_resources(&db_path);
    }

    #[test]
    fn test_insert_single_message() {
        let (mut cache, _, db_path) = setup_test_cache(10);

        let message = create_test_message("test_exchange", "test_key", "Test message", 0);
        let timestamp = message.timestamp;
        cache.insert(message);

        assert_eq!(cache.size(), 1);
        assert!(cache.messages.contains_key(&timestamp));

        let recent_messages = cache.get_recent(10);
        assert_eq!(recent_messages.len(), 1);
        assert_eq!(recent_messages[0].exchange, "test_exchange");
        assert_eq!(recent_messages[0].routing_key, "test_key");
        assert_eq!(recent_messages[0].message, "Test message");

        cleanup_test_resources(&db_path);
    }

    #[test]
    fn test_batch_insert() {
        let (mut cache, _, db_path) = setup_test_cache(10);

        let messages = vec![
            create_test_message("exchange1", "key1", "Message 1", 0),
            create_test_message("exchange2", "key2", "Message 2", 1),
            create_test_message("exchange3", "key3", "Message 3", 2),
        ];

        cache.batch_insert(messages);

        assert_eq!(cache.size(), 3);

        let recent_messages = cache.get_recent(10);
        assert_eq!(recent_messages.len(), 3);

        assert_eq!(recent_messages[0].exchange, "exchange3");
        assert_eq!(recent_messages[1].exchange, "exchange2");
        assert_eq!(recent_messages[2].exchange, "exchange1");

        cleanup_test_resources(&db_path);
    }

    #[test]
    fn test_cache_overflow() {
        let (mut cache, db_arc, db_path) = setup_test_cache(5);

        for i in 0..6 {
            let message = create_test_message(
                "test_exchange",
                &format!("key_{}", i),
                &format!("Message {}", i),
                i,
            );
            cache.insert(message);
        }

        assert_eq!(cache.size(), 5);

        {
            let db = db_arc.lock().unwrap();
            let db_messages = db.get_recent_messages(10).unwrap();
            assert_eq!(db_messages.len(), 1);
            assert_eq!(db_messages[0].routing_key, "key_0");
        }

        cleanup_test_resources(&db_path);
    }

    #[test]
    fn test_get_by_exchange() {
        let (mut cache, _, db_path) = setup_test_cache(10);

        let messages = vec![
            create_test_message("exchange1", "key1", "Message 1 for exchange1", 0),
            create_test_message("exchange2", "key1", "Message 1 for exchange2", 1),
            create_test_message("exchange1", "key2", "Message 2 for exchange1", 2),
            create_test_message("exchange2", "key2", "Message 2 for exchange2", 3),
        ];

        cache.batch_insert(messages);

        let exchange1_messages = cache.get_by_exchange("exchange1", 10);
        assert_eq!(exchange1_messages.len(), 2);
        for msg in &exchange1_messages {
            assert_eq!(msg.exchange, "exchange1");
        }

        let exchange2_messages = cache.get_by_exchange("exchange2", 10);
        assert_eq!(exchange2_messages.len(), 2);
        for msg in &exchange2_messages {
            assert_eq!(msg.exchange, "exchange2");
        }

        let non_existent = cache.get_by_exchange("non_existent", 10);
        assert_eq!(non_existent.len(), 0);

        cleanup_test_resources(&db_path);
    }

    #[test]
    fn test_get_recent_with_limit() {
        let (mut cache, _, db_path) = setup_test_cache(20);

        for i in 0..10 {
            let message = create_test_message(
                "test_exchange",
                &format!("key_{}", i),
                &format!("Message {}", i),
                i,
            );
            cache.insert(message);
        }

        let limited_messages = cache.get_recent(5);
        assert_eq!(limited_messages.len(), 5);

        for i in 0..limited_messages.len() - 1 {
            assert!(
                limited_messages[i].timestamp > limited_messages[i + 1].timestamp,
                "消息未按时间戳降序排列"
            );
        }

        cleanup_test_resources(&db_path);
    }

    #[test]
    fn test_remove_message() {
        let (mut cache, _, db_path) = setup_test_cache(10);

        let message = create_test_message("test_exchange", "test_key", "Test message", 0);
        let timestamp = message.timestamp;
        cache.insert(message);

        assert_eq!(cache.size(), 1);

        let removed = cache.remove(timestamp);
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().routing_key, "test_key");

        assert_eq!(cache.size(), 0);
        assert_eq!(cache.get_recent(10).len(), 0);

        let not_found = cache.remove(12345);
        assert!(not_found.is_none());

        cleanup_test_resources(&db_path);
    }

    #[test]
    fn test_remove_batch() {
        let (mut cache, _, db_path) = setup_test_cache(10);

        let mut timestamps = Vec::new();
        for i in 0..5 {
            let message = create_test_message(
                "test_exchange",
                &format!("key_{}", i),
                &format!("Message {}", i),
                i,
            );
            timestamps.push(message.timestamp);
            cache.insert(message);
        }

        let timestamps_to_remove = &timestamps[0..3];
        let removed = cache.remove_batch(timestamps_to_remove);

        assert_eq!(removed.len(), 3);
        assert_eq!(cache.size(), 2);

        let remaining = cache.get_recent(10);
        assert_eq!(remaining.len(), 2);
        assert_eq!(remaining[0].routing_key, "key_4");
        assert_eq!(remaining[1].routing_key, "key_3");

        cleanup_test_resources(&db_path);
    }

    #[test]
    fn test_flush_all_to_db() {
        let (mut cache, db_arc, db_path) = setup_test_cache(10);

        for i in 0..5 {
            let message = create_test_message(
                "test_exchange",
                &format!("key_{}", i),
                &format!("Message {}", i),
                i,
            );
            cache.insert(message);
        }

        cache.flush_all_to_db();

        assert_eq!(cache.size(), 0);
        assert_eq!(cache.get_recent(10).len(), 0);

        {
            let db = db_arc.lock().unwrap();
            let db_messages = db.get_recent_messages(10).unwrap();
            assert_eq!(db_messages.len(), 5);
        }

        cleanup_test_resources(&db_path);
    }

    #[test]
    fn test_cache_ordering() {
        let (mut cache, _, db_path) = setup_test_cache(10);

        let message1 = create_test_message("test_exchange", "key_1", "Message 1", 20);
        let message2 = create_test_message("test_exchange", "key_2", "Message 2", 10);
        let message3 = create_test_message("test_exchange", "key_3", "Message 3", 30);

        cache.insert(message1);
        cache.insert(message2);
        cache.insert(message3);

        let messages = cache.get_recent(10);
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0].routing_key, "key_3");
        assert_eq!(messages[1].routing_key, "key_1");
        assert_eq!(messages[2].routing_key, "key_2");

        cleanup_test_resources(&db_path);
    }

    #[test]
    fn test_performance_comparison() {
        let (mut cache, db_arc, db_path) = setup_test_cache(1000);

        let mut messages = Vec::new();
        for i in 0..100 {
            messages.push(create_test_message(
                "perf_test",
                &format!("key_{}", i),
                &format!("Performance test message {}", i),
                i,
            ));
        }

        let start_cache = std::time::Instant::now();
        for message in messages.clone() {
            cache.insert(message);
        }
        let cache_duration = start_cache.elapsed();

        cache.flush_all_to_db();

        let start_db = std::time::Instant::now();
        {
            let mut db = db_arc.lock().unwrap();
            db.batch_insert(&messages).unwrap();
        }
        let db_duration = start_db.elapsed();

        println!("内存缓存100条消息耗时: {:?}", cache_duration);
        println!("直接数据库插入100条消息耗时: {:?}", db_duration);

        assert!(cache_duration < db_duration);

        cleanup_test_resources(&db_path);
    }

    #[test]
    fn test_cache_concurrent_access() {
        use std::thread;

        let (cache, _db_arc, db_path) = setup_test_cache(100);
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
                cache.batch_insert(messages);
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        {
            let cache = cache_arc.lock().unwrap();

            let total_expected = threads * messages_per_thread;
            assert_eq!(cache.size(), total_expected);

            for thread_id in 0..threads {
                let thread_messages = cache.get_by_exchange(&format!("thread_{}", thread_id), 100);
                assert_eq!(thread_messages.len(), messages_per_thread);
            }
        }

        cleanup_test_resources(&db_path);
    }

    #[test]
    fn test_automatic_flush_to_db() {
        let (mut cache, db_arc, db_path) = setup_test_cache(4);

        for i in 0..4 {
            cache.insert(create_test_message(
                "test_exchange",
                &format!("key_{}", i),
                &format!("Message {}", i),
                i,
            ));
        }

        assert_eq!(cache.size(), 4);
        {
            let db = db_arc.lock().unwrap();
            assert_eq!(db.get_recent_messages(10).unwrap().len(), 0);
        }

        cache.insert(create_test_message(
            "test_exchange",
            "key_trigger",
            "This message triggers flush",
            100,
        ));

        assert_eq!(cache.size(), 4);

        {
            let db = db_arc.lock().unwrap();
            let db_messages = db.get_recent_messages(10).unwrap();
            assert_eq!(db_messages.len(), 1);
            assert_eq!(db_messages[0].routing_key, "key_0");
        }

        cleanup_test_resources(&db_path);
    }
}
