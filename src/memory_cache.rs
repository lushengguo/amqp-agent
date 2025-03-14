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
