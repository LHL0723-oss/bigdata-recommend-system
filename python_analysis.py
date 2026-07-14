from kafka import KafkaConsumer
import json
from collections import defaultdict

# 配置
KAFKA_TOPIC = "student_answer"
KAFKA_SERVERS = "localhost:9092"

# 统计数据
student_scores = defaultdict(list)

print("[分析器] 开始从 Kafka 消费数据...")
print("[分析器] 按 Ctrl+C 停止\n")

# 创建消费者
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVERS,
    auto_offset_reset='latest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

count = 0
try:
    for message in consumer:
        data = message.value
        student_id = data.get("student_id")
        score = data.get("score", 0)
        
        student_scores[student_id].append(score)
        count += 1
        
        if count % 10 == 0:  # 每10条输出一次统计
            print("\n" + "="*50)
            print(f"已处理 {count} 条数据")
            print("-"*50)
            print(f"{'学生ID':<12} {'平均分':<10} {'答题数':<10}")
            print("-"*50)
            for sid, scores in list(student_scores.items())[:10]:
                avg = sum(scores) / len(scores)
                print(f"{sid:<12} {avg:<10.1f} {len(scores):<10}")
            print("="*50 + "\n")
            
except KeyboardInterrupt:
    print(f"\n[分析器] 共处理 {count} 条数据，已停止")
    consumer.close()
