from kafka import KafkaConsumer
import json
import redis
import pymysql
import time

print("[推荐器] ========== 启动实时推荐 ==========")

# ===== 连接 Redis =====
try:
    r = redis.Redis(host='localhost', port=6379, db=0)
    model_ready = r.get("model_ready")
    if not model_ready:
        print("[推荐器] ⚠️ 模型尚未训练，请先运行 train_recommend.py")
        exit(1)
    print("[推荐器] ✅ 模型已加载")
except Exception as e:
    print(f"[推荐器] ❌ Redis 连接失败: {e}")
    exit(1)

# ===== 连接 MySQL =====
def save_to_mysql(student_id, recommendations):
    try:
        conn = pymysql.connect(
            host='localhost',
            user='root',
            password='123456',  # 如果 MySQL 有密码，在这里填
            database='learning',
            charset='utf8mb4'
        )
        cursor = conn.cursor()
        
        # 创建表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS t_recommended (
                id INT AUTO_INCREMENT PRIMARY KEY,
                student_id VARCHAR(50),
                recommendations TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        sql = "INSERT INTO t_recommended (student_id, recommendations) VALUES (%s, %s)"
        cursor.execute(sql, (student_id, json.dumps(recommendations, ensure_ascii=False)))
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"[推荐器] MySQL 保存失败: {e}")
        return False

# ===== 从 Kafka 消费数据并推荐 =====
consumer = KafkaConsumer(
    "student_answer",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="latest",
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("[推荐器] 开始监听 Kafka 数据...")
print("[推荐器] 按 Ctrl+C 停止\n")

processed = set()
count = 0

try:
    for msg in consumer:
        data = msg.value
        student_id = data.get("student_id")
        
        if student_id in processed:
            continue
        
        rec_data = r.get(f"rec:{student_id}")
        
        if rec_data:
            recommendations = json.loads(rec_data)
            print(f"[推荐] {student_id} → {recommendations}")
            
            if save_to_mysql(student_id, recommendations):
                print(f"   ✅ 已保存到 MySQL")
            else:
                print(f"   ⚠️ 保存失败")
            
            processed.add(student_id)
            count += 1
        else:
            print(f"[推荐] {student_id} 暂无推荐规则")

except KeyboardInterrupt:
    print(f"\n[推荐器] 已处理 {count} 个学生的推荐，已停止")
