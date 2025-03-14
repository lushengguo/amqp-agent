use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use super::models::Message;
use super::db::DB;
use tracing::{info, warn};

pub struct MemoryCache {
    // 使用 BTreeMap 因为它能够按 timestamp 自动排序
    messages: BTreeMap<u32, Message>,
    // 当前缓存大小（以消息数量计）
    current_size: usize,
    // 最大缓存大小（以消息数量计）
    max_size: usize,
    // 数据库连接，用于溢出数据持久化
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
        // 如果达到内存限制，将最旧的数据转移到数据库
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

    // 将最旧的 batch_size 条数据刷新到数据库
    fn flush_oldest_to_db(&mut self) {
        let batch_size = self.max_size / 4; // 每次刷新 25% 的数据
        let mut to_remove = Vec::new();
        let mut to_db = Vec::new();

        // 收集最旧的数据
        for (timestamp, message) in self.messages.iter() {
            if to_remove.len() >= batch_size {
                break;
            }
            to_remove.push(*timestamp);
            to_db.push(message.clone());
        }

        // 写入数据库
        if let Ok(mut db) = self.db.lock() {
            match db.batch_insert(&to_db) {
                Ok(_) => {
                    info!("成功将 {} 条消息刷新到数据库", to_db.len());
                    // 从内存中移除
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

    // 获取最近的 n 条消息
    pub fn get_recent(&self, n: usize) -> Vec<Message> {
        self.messages
            .iter()
            .rev()
            .take(n)
            .map(|(_, message)| message.clone())
            .collect()
    }

    // 获取特定 exchange 的最近消息
    pub fn get_by_exchange(&self, exchange: &str, n: usize) -> Vec<Message> {
        self.messages
            .iter()
            .rev()
            .filter(|(_, msg)| msg.exchange == exchange)
            .take(n)
            .map(|(_, message)| message.clone())
            .collect()
    }

    // 手动触发刷新到数据库
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

    // 获取当前缓存大小
    pub fn size(&self) -> usize {
        self.current_size
    }

    // 获取最大缓存大小
    pub fn max_size(&self) -> usize {
        self.max_size
    }

    // 删除指定 timestamp 的消息
    pub fn remove(&mut self, timestamp: u32) -> Option<Message> {
        if let Some(message) = self.messages.remove(&timestamp) {
            self.current_size -= 1;
            Some(message)
        } else {
            None
        }
    }

    // 删除多个消息
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
    use std::time::{SystemTime, UNIX_EPOCH};
    use std::fs;

    // 创建测试消息的辅助函数
    fn create_test_message(exchange: &str, routing_key: &str, message_content: &str, timestamp_offset: u32) -> Message {
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

    // 创建测试用的内存缓存和数据库
    fn setup_test_cache(max_size: usize) -> (MemoryCache, Arc<Mutex<DB>>, String) {
        // 创建临时数据库
        let db_path = format!("test_cache_db_{}.sqlite", rand::random::<u32>());
        let db = DB::new_with_path(&db_path).unwrap();
        let db_arc = Arc::new(Mutex::new(db));
        
        // 创建内存缓存
        let cache = MemoryCache::new(max_size, db_arc.clone());
        
        (cache, db_arc, db_path)
    }

    // 清理测试资源
    fn cleanup_test_resources(db_path: &str) {
        let _ = fs::remove_file(db_path);
    }

    #[test]
    fn test_cache_creation() {
        let (cache, _, db_path) = setup_test_cache(10);
        
        // 验证初始状态
        assert_eq!(cache.size(), 0);
        assert_eq!(cache.max_size(), 10);
        assert!(cache.messages.is_empty());
        
        cleanup_test_resources(&db_path);
    }

    #[test]
    fn test_insert_single_message() {
        let (mut cache, _, db_path) = setup_test_cache(10);
        
        // 插入单条消息
        let message = create_test_message("test_exchange", "test_key", "Test message", 0);
        let timestamp = message.timestamp;
        cache.insert(message);
        
        // 验证缓存状态
        assert_eq!(cache.size(), 1);
        assert!(cache.messages.contains_key(&timestamp));
        
        // 验证可以获取消息
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
        
        // 创建多条测试消息
        let messages = vec![
            create_test_message("exchange1", "key1", "Message 1", 0),
            create_test_message("exchange2", "key2", "Message 2", 1),
            create_test_message("exchange3", "key3", "Message 3", 2),
        ];
        
        // 批量插入
        cache.batch_insert(messages);
        
        // 验证缓存状态
        assert_eq!(cache.size(), 3);
        
        // 获取消息并验证
        let recent_messages = cache.get_recent(10);
        assert_eq!(recent_messages.len(), 3);
        
        // 验证消息是按时间戳降序排序的
        assert_eq!(recent_messages[0].exchange, "exchange3");
        assert_eq!(recent_messages[1].exchange, "exchange2");
        assert_eq!(recent_messages[2].exchange, "exchange1");
        
        cleanup_test_resources(&db_path);
    }

    #[test]
    fn test_cache_overflow() {
        // 创建一个小容量的缓存(5)
        let (mut cache, db_arc, db_path) = setup_test_cache(5);
        
        // 插入6条消息，触发溢出
        for i in 0..6 {
            let message = create_test_message(
                "test_exchange",
                &format!("key_{}", i),
                &format!("Message {}", i),
                i
            );
            cache.insert(message);
        }
        
        // 缓存大小应该仍然是5
        assert_eq!(cache.size(), 5);
        
        // 应该有1条消息被刷新到数据库
        {
            let db = db_arc.lock().unwrap();
            let db_messages = db.get_recent_messages(10).unwrap();
            assert_eq!(db_messages.len(), 1);
            assert_eq!(db_messages[0].routing_key, "key_0"); // 最旧的消息
        }
        
        cleanup_test_resources(&db_path);
    }

    #[test]
    fn test_get_by_exchange() {
        let (mut cache, _, db_path) = setup_test_cache(10);
        
        // 插入不同交换机的消息
        let messages = vec![
            create_test_message("exchange1", "key1", "Message 1 for exchange1", 0),
            create_test_message("exchange2", "key1", "Message 1 for exchange2", 1),
            create_test_message("exchange1", "key2", "Message 2 for exchange1", 2),
            create_test_message("exchange2", "key2", "Message 2 for exchange2", 3),
        ];
        
        cache.batch_insert(messages);
        
        // 获取特定交换机的消息
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
        
        // 不存在的交换机
        let non_existent = cache.get_by_exchange("non_existent", 10);
        assert_eq!(non_existent.len(), 0);
        
        cleanup_test_resources(&db_path);
    }

    #[test]
    fn test_get_recent_with_limit() {
        let (mut cache, _, db_path) = setup_test_cache(20);
        
        // 插入10条消息
        for i in 0..10 {
            let message = create_test_message(
                "test_exchange",
                &format!("key_{}", i),
                &format!("Message {}", i),
                i
            );
            cache.insert(message);
        }
        
        // 测试限制
        let limited_messages = cache.get_recent(5);
        assert_eq!(limited_messages.len(), 5);
        
        // 验证是按时间戳降序排序的（最新的先返回）
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
        
        // 插入消息
        let message = create_test_message("test_exchange", "test_key", "Test message", 0);
        let timestamp = message.timestamp;
        cache.insert(message);
        
        // 验证消息已插入
        assert_eq!(cache.size(), 1);
        
        // 删除消息
        let removed = cache.remove(timestamp);
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().routing_key, "test_key");
        
        // 验证消息已删除
        assert_eq!(cache.size(), 0);
        assert_eq!(cache.get_recent(10).len(), 0);
        
        // 尝试删除不存在的消息
        let not_found = cache.remove(12345);
        assert!(not_found.is_none());
        
        cleanup_test_resources(&db_path);
    }

    #[test]
    fn test_remove_batch() {
        let (mut cache, _, db_path) = setup_test_cache(10);
        
        // 插入5条消息
        let mut timestamps = Vec::new();
        for i in 0..5 {
            let message = create_test_message(
                "test_exchange",
                &format!("key_{}", i),
                &format!("Message {}", i),
                i
            );
            timestamps.push(message.timestamp);
            cache.insert(message);
        }
        
        // 删除前3条消息
        let timestamps_to_remove = &timestamps[0..3];
        let removed = cache.remove_batch(timestamps_to_remove);
        
        // 验证删除数量
        assert_eq!(removed.len(), 3);
        assert_eq!(cache.size(), 2);
        
        // 验证剩余消息
        let remaining = cache.get_recent(10);
        assert_eq!(remaining.len(), 2);
        assert_eq!(remaining[0].routing_key, "key_4");
        assert_eq!(remaining[1].routing_key, "key_3");
        
        cleanup_test_resources(&db_path);
    }

    #[test]
    fn test_flush_all_to_db() {
        let (mut cache, db_arc, db_path) = setup_test_cache(10);
        
        // 插入5条消息
        for i in 0..5 {
            let message = create_test_message(
                "test_exchange",
                &format!("key_{}", i),
                &format!("Message {}", i),
                i
            );
            cache.insert(message);
        }
        
        // 刷新所有消息到数据库
        cache.flush_all_to_db();
        
        // 验证缓存已清空
        assert_eq!(cache.size(), 0);
        assert_eq!(cache.get_recent(10).len(), 0);
        
        // 验证数据库有5条消息
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
        
        // 插入消息，确保时间戳不按顺序
        let message1 = create_test_message("test_exchange", "key_1", "Message 1", 20);
        let message2 = create_test_message("test_exchange", "key_2", "Message 2", 10);
        let message3 = create_test_message("test_exchange", "key_3", "Message 3", 30);
        
        cache.insert(message1);
        cache.insert(message2);
        cache.insert(message3);
        
        // 获取消息，应该按时间戳降序排序
        let messages = cache.get_recent(10);
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0].routing_key, "key_3"); // 最新的（时间戳最大）
        assert_eq!(messages[1].routing_key, "key_1");
        assert_eq!(messages[2].routing_key, "key_2"); // 最旧的（时间戳最小）
        
        cleanup_test_resources(&db_path);
    }

    #[test]
    fn test_performance_comparison() {
        let (mut cache, db_arc, db_path) = setup_test_cache(1000);
        
        // 准备100条测试消息
        let mut messages = Vec::new();
        for i in 0..100 {
            messages.push(create_test_message(
                "perf_test",
                &format!("key_{}", i),
                &format!("Performance test message {}", i),
                i
            ));
        }
        
        // 测试内存缓存的性能
        let start_cache = std::time::Instant::now();
        for message in messages.clone() {
            cache.insert(message);
        }
        let cache_duration = start_cache.elapsed();
        
        // 清空缓存
        cache.flush_all_to_db();
        
        // 测试直接数据库插入的性能
        let start_db = std::time::Instant::now();
        {
            let mut db = db_arc.lock().unwrap();
            db.batch_insert(&messages).unwrap();
        }
        let db_duration = start_db.elapsed();
        
        // 输出性能对比
        println!("内存缓存100条消息耗时: {:?}", cache_duration);
        println!("直接数据库插入100条消息耗时: {:?}", db_duration);
        
        // 内存缓存应该比直接数据库插入快
        assert!(cache_duration < db_duration);
        
        cleanup_test_resources(&db_path);
    }

    #[test]
    fn test_cache_concurrent_access() {
        use std::thread;
        
        let (cache, _db_arc, db_path) = setup_test_cache(100);
        let cache_arc = Arc::new(Mutex::new(cache));
        
        // 创建多个线程并发访问缓存
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
                        (thread_id * 100 + i) as u32
                    ));
                }
                
                let mut cache = cache_clone.lock().unwrap();
                cache.batch_insert(messages);
            });
            
            handles.push(handle);
        }
        
        // 等待所有线程完成
        for handle in handles {
            handle.join().unwrap();
        }
        
        // 验证结果
        {
            let cache = cache_arc.lock().unwrap();
            
            // 总共应有 threads * messages_per_thread 条消息
            let total_expected = threads * messages_per_thread;
            assert_eq!(cache.size(), total_expected);
            
            // 验证每个线程的消息都在缓存中
            for thread_id in 0..threads {
                let thread_messages = cache.get_by_exchange(&format!("thread_{}", thread_id), 100);
                assert_eq!(thread_messages.len(), messages_per_thread);
            }
        }
        
        cleanup_test_resources(&db_path);
    }
    
    #[test]
    fn test_automatic_flush_to_db() {
        // 创建一个小容量的缓存，这样我们可以测试自动刷新到数据库的功能
        let (mut cache, db_arc, db_path) = setup_test_cache(4);
        
        // 首先插入max_size条消息，不会触发刷新
        for i in 0..4 {
            cache.insert(create_test_message(
                "test_exchange",
                &format!("key_{}", i),
                &format!("Message {}", i),
                i
            ));
        }
        
        // 验证缓存中有4条消息，数据库中有0条
        assert_eq!(cache.size(), 4);
        {
            let db = db_arc.lock().unwrap();
            assert_eq!(db.get_recent_messages(10).unwrap().len(), 0);
        }
        
        // 再插入一条消息，会触发自动刷新
        cache.insert(create_test_message(
            "test_exchange",
            "key_trigger",
            "This message triggers flush",
            100
        ));
        
        // 验证缓存仍有4条消息（最旧的1条被刷新到数据库）
        assert_eq!(cache.size(), 4);
        
        // 验证数据库中现在有1条消息（最旧的）
        {
            let db = db_arc.lock().unwrap();
            let db_messages = db.get_recent_messages(10).unwrap();
            assert_eq!(db_messages.len(), 1);
            assert_eq!(db_messages[0].routing_key, "key_0"); // 最旧的消息
        }
        
        cleanup_test_resources(&db_path);
    }
}
