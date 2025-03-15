#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import socket
import json
import time
import random
import datetime
import argparse
from typing import Dict, Any, List, Optional
import threading

# 默认配置
DEFAULT_HOST = "127.0.0.1"  # Rust服务地址
DEFAULT_PORT = 8080  # Rust服务端口
DEFAULT_INTERVAL = 2.0  # 发送间隔（秒）
DEFAULT_COUNT = -1  # 发送消息数量,-1表示无限发送

# RabbitMQ配置
RABBITMQ_URL = "amqp://guest:guest@127.0.0.1:5672"
EXCHANGES = ["test_exchange", "logs_exchange", "data_exchange"]
ROUTING_KEYS = ["test_key", "info", "error", "data.priority.high", "data.priority.low"]

class MessageGenerator:
    """生成测试消息的类"""
    
    def __init__(self):
        self.message_id = 0
    
    def generate_message(self) -> Dict[str, Any]:
        """生成一条测试消息"""
        self.message_id += 1
        
        # 随机选择交换机和路由键
        exchange = random.choice(EXCHANGES)
        routing_key = random.choice(ROUTING_KEYS)
        
        # 获取当前时间戳
        timestamp = int(time.time())
        
        # 生成消息内容
        content = {
            "id": self.message_id,
            "type": "test_message",
            "content": f"测试消息 #{self.message_id},发送时间: {datetime.datetime.now().isoformat()}",
            "data": {
                "value": random.randint(1, 100),
                "temperature": round(random.uniform(20.0, 30.0), 2),
                "status": random.choice(["ok", "warning", "error"])
            }
        }
        
        # 构造完整消息
        message = {
            "url": RABBITMQ_URL,
            "exchange": exchange,
            "routing_key": routing_key,
            "message": json.dumps(content, ensure_ascii=False),
            "timestamp": timestamp
        }
        
        return message

class TCPClient:
    """通过TCP发送消息的客户端"""
    
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.socket = None
        self.connected = False
    
    def connect(self) -> bool:
        """连接到服务器"""
        if self.connected and self.socket:
            return True
            
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.host, self.port))
            self.connected = True
            print(f"已连接到服务器 {self.host}:{self.port}")
            return True
        except socket.error as e:
            print(f"连接服务器失败: {e}")
            self.connected = False
            return False
    
    def send_message(self, message: Dict[str, Any]) -> bool:
        """发送消息到服务器"""
        if not self.connected and not self.connect():
            return False
            
        try:
            # 将消息转换为JSON字符串,并添加换行符
            json_message = json.dumps(message, ensure_ascii=False) + "\n"
            self.socket.sendall(json_message.encode('utf-8'))
            return True
        except socket.error as e:
            print(f"发送消息失败: {e}")
            self.connected = False
            return False
    
    def close(self):
        """关闭连接"""
        if self.socket:
            self.socket.close()
            self.socket = None
            self.connected = False
            print("已关闭连接")

class BatchSender(threading.Thread):
    """批量发送消息的线程类"""
    
    def __init__(self, host: str, port: int, batch_size: int, interval: float):
        super().__init__()
        self.host = host
        self.port = port
        self.batch_size = batch_size
        self.interval = interval
        self.stop_event = threading.Event()
        self.client = TCPClient(host, port)
        self.generator = MessageGenerator()
        
    def run(self):
        """运行批量发送线程"""
        try:
            while not self.stop_event.is_set():
                batch = [self.generator.generate_message() for _ in range(self.batch_size)]
                
                # 统计发送成功的消息数量
                success_count = 0
                for message in batch:
                    if self.client.send_message(message):
                        success_count += 1
                
                print(f"批量发送完成: {success_count}/{self.batch_size} 消息发送成功")
                
                # 等待下一个发送间隔
                self.stop_event.wait(self.interval)
                
        finally:
            self.client.close()
    
    def stop(self):
        """停止批量发送线程"""
        self.stop_event.set()
        self.join()

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="发送测试数据到Rust服务")
    parser.add_argument("--host", default=DEFAULT_HOST, help=f"服务器地址 (默认: {DEFAULT_HOST})")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"服务器端口 (默认: {DEFAULT_PORT})")
    parser.add_argument("--interval", type=float, default=DEFAULT_INTERVAL, help=f"发送间隔(秒) (默认: {DEFAULT_INTERVAL})")
    parser.add_argument("--count", type=int, default=DEFAULT_COUNT, help=f"发送消息数量, -1表示无限发送 (默认: {DEFAULT_COUNT})")
    parser.add_argument("--batch", type=int, default=1, help="批量发送消息数量 (默认: 1)")
    parser.add_argument("--batch-mode", action="store_true", help="启用批量发送模式")
    
    args = parser.parse_args()
    
    print("消息发送器 - 向Rust服务发送测试数据")
    print(f"目标: {args.host}:{args.port}")
    print(f"RabbitMQ: {RABBITMQ_URL}")
    
    if args.batch_mode:
        print(f"批量发送模式: 每{args.interval}秒发送{args.batch}条消息")
        batch_sender = BatchSender(args.host, args.port, args.batch, args.interval)
        
        try:
            batch_sender.start()
            print("批量发送已开始,按Ctrl+C停止...")
            # 保持主线程运行
            while batch_sender.is_alive():
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n接收到停止信号,正在停止发送...")
            batch_sender.stop()
            
    else:
        # 单条消息发送模式
        client = TCPClient(args.host, args.port)
        generator = MessageGenerator()
        
        sent_count = 0
        try:
            while args.count == -1 or sent_count < args.count:
                message = generator.generate_message()
                # 解析消息内容以获取 ID
                content = json.loads(message['message'])
                print(f"\n发送消息 #{content['id']}:")
                print(f"  交换机: {message['exchange']}")
                print(f"  路由键: {message['routing_key']}")
                print(f"  消息内容: {message['message'][:50]}...")
                
                if client.send_message(message):
                    sent_count += 1
                    print(f"  状态: 成功 (已发送 {sent_count} 条)")
                else:
                    print("  状态: 失败")
                
                if args.count == -1 or sent_count < args.count:
                    print(f"等待 {args.interval} 秒后发送下一条...")
                    time.sleep(args.interval)
                    
        except KeyboardInterrupt:
            print("\n接收到停止信号,正在停止发送...")
        finally:
            client.close()
            print(f"总计发送: {sent_count} 条消息")

if __name__ == "__main__":
    main() 