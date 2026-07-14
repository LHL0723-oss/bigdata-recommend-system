import json
import random
import time
from kafka import KafkaProducer

KAFKA_TOPIC = "student_answer"
KAFKA_SERVERS = "localhost:9092"

# 模拟数据范围
STUDENT_IDS = [f"学生ID_{i}" for i in range(1, 101)]
QUESTION_IDS = [f"题目ID_{i}" for i in range(1, 51)]
TEXTBOOK_IDS = [f"教材ID_{i}" for i in range(1, 6)]
GRADE_IDS = [f"年级ID_{i}" for i in range(1, 4)]
SUBJECT_IDS = [f"科目ID_{i}" for i in range(1, 10)]
CHAPTER_IDS = [f"章节ID_{i}" for i in range(1, 21)]

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVERS,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
)

def generate_answer():
    return {
        "student_id": random.choice(STUDENT_IDS),
        "question_id": random.choice(QUESTION_IDS),   # ✅ 必须有
        "textbook_id": random.choice(TEXTBOOK_IDS),
        "grade_id": random.choice(GRADE_IDS),
        "subject_id": random.choice(SUBJECT_IDS),
        "chapter_id": random.choice(CHAPTER_IDS),     # ✅ 必须有
        "score": random.randint(0, 100),              # ✅ 必须有
        "answer_time": time.strftime("%Y-%m-%d %H:%M:%S"),
        "ts": int(time.time() * 1000)
    }

if __name__ == "__main__":
    print("[数据生成器] 开始发送数据到Kafka...")
    count = 0
    try:
        while True:
            data = generate_answer()
            producer.send(KAFKA_TOPIC, value=data)
            count += 1
            print(f"[{count}] 发送: {json.dumps(data, ensure_ascii=False)}")
            time.sleep(random.uniform(0.3, 0.8))
    except KeyboardInterrupt:
        print(f"\n[数据生成器] 共发送 {count} 条数据")
        producer.close()
