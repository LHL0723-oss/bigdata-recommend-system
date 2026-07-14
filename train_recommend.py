from kafka import KafkaConsumer
import json
from collections import defaultdict
import redis
import numpy as np

print("[训练器] ========== 启动推荐模型训练 ==========")

# ===== 1. 从 Kafka 消费历史数据 =====
consumer = KafkaConsumer(
    "student_answer",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("[训练器] 正在从 Kafka 读取历史答题数据...")

# 收集数据
student_answers = defaultdict(list)  # 学生ID -> [题目ID, ...]
question_scores = defaultdict(list)  # 题目ID -> [得分, ...]

count = 0
for msg in consumer:
    data = msg.value
    student_id = data.get("student_id")
    question_id = data.get("question_id")
    score = data.get("score", 0)
    
    student_answers[student_id].append(question_id)
    question_scores[question_id].append(score)
    count += 1
    
    if count >= 1000:  # 取1000条数据训练
        break

consumer.close()
print(f"[训练器] 已读取 {count} 条数据")

# ===== 2. 计算每个题目的平均分（难度系数） =====
print("[训练器] 正在计算题目难度...")
question_avg = {}
for qid, scores in question_scores.items():
    question_avg[qid] = sum(scores) / len(scores)

# ===== 3. 计算每个学生最擅长的题型（偏好） =====
print("[训练器] 正在分析学生偏好...")
student_preference = {}
for sid, questions in student_answers.items():
    if len(questions) >= 3:  # 至少答过3题
        # 找该学生得分最高的题目类型
        student_preference[sid] = questions[-3:]  # 最近3题作为偏好

# ===== 4. 生成推荐规则 =====
print("[训练器] 正在生成推荐规则...")
recommend_rules = {}

for sid, pref_questions in student_preference.items():
    # 推荐：题目平均分在50-80之间（有挑战性但不太难）
    candidates = [qid for qid, avg in question_avg.items() 
                  if 50 <= avg <= 80 and qid not in pref_questions]
    recommend_rules[sid] = candidates[:5]  # 推荐5题

print(f"[训练器] 为 {len(recommend_rules)} 个学生生成了推荐规则")

# ===== 5. 存储到 Redis =====
print("[训练器] 正在存入 Redis...")
try:
    r = redis.Redis(host='localhost', port=6379, db=0)
    
    # 存储推荐规则
    for sid, recs in recommend_rules.items():
        r.set(f"rec:{sid}", json.dumps(recs))
    
    # 标记模型已就绪
    r.set("model_ready", "true")
    r.set("model_time", str(__import__('time').time()))
    
    print(f"[训练器] ✅ 已存入 {len(recommend_rules)} 条推荐规则到 Redis")
    print("[训练器] ✅ 模型已就绪！")

except Exception as e:
    print(f"[训练器] ❌ Redis 连接失败: {e}")
    print("[训练器] 请先启动 Redis: sudo systemctl start redis-server")

print("[训练器] ========== 训练完成 ==========")
