import pymysql
import json
from collections import Counter

print("="*50)
print("  离线分析 - 热点题与推荐分布")
print("="*50)

try:
    conn = pymysql.connect(
        host='localhost',
        user='root',
        password='123456',  # ⚠️ 改成你的密码
        database='learning',
        charset='utf8mb4'
    )
    cursor = conn.cursor()
    print("✅ 数据库连接成功")
except Exception as e:
    print(f"❌ 数据库连接失败: {e}")
    exit(1)

# 检查数据
cursor.execute("SELECT COUNT(*) FROM t_recommended")
count = cursor.fetchone()[0]
print(f"📊 t_recommended 表中有 {count} 条记录")

if count == 0:
    print("❌ 没有数据，请先运行 realtime_recommend.py")
    exit(1)

# 统计推荐题目
print("\n[分析1] 统计推荐题目分布")
cursor.execute("SELECT student_id, recommendations FROM t_recommended")
rows = cursor.fetchall()

all_questions = []
for row in rows:
    try:
        recs = json.loads(row[1])
        all_questions.extend(recs)
    except:
        pass

question_count = Counter(all_questions)
print(f"📊 共统计到 {len(question_count)} 道不同的题目")
print(f"📊 总推荐次数: {sum(question_count.values())} 次")

# 保存热点题 Top 50
print("\n[保存] 热点题 Top 50")
top_50 = question_count.most_common(50)

# 删除旧表（如果存在）
cursor.execute("DROP TABLE IF EXISTS t_hot_questions")

# 创建新表（用 ranking 代替 rank）
cursor.execute("""
    CREATE TABLE t_hot_questions (
        id INT AUTO_INCREMENT PRIMARY KEY,
        question_id VARCHAR(50),
        frequency INT,
        ranking INT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
""")

# 插入数据
for rank, (qid, freq) in enumerate(top_50, 1):
    cursor.execute(
        "INSERT INTO t_hot_questions (question_id, frequency, ranking) VALUES (%s, %s, %s)",
        (qid, freq, rank)
    )

conn.commit()
print(f"✅ 已保存 {len(top_50)} 条热点题分析结果")

# 保存推荐分布
print("\n[保存] 推荐分布")
cursor.execute("DROP TABLE IF EXISTS t_recommend_distribution")
cursor.execute("""
    CREATE TABLE t_recommend_distribution (
        id INT AUTO_INCREMENT PRIMARY KEY,
        question_id VARCHAR(50),
        recommend_count INT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
""")

for qid, count_val in question_count.most_common():
    cursor.execute(
        "INSERT INTO t_recommend_distribution (question_id, recommend_count) VALUES (%s, %s)",
        (qid, count_val)
    )

conn.commit()
print(f"✅ 已保存 {len(question_count)} 条推荐分布数据")

# 显示结果
print("\n" + "="*50)
print("  📊 热点题 Top 10")
print("="*50)
for rank, (qid, freq) in enumerate(top_50[:10], 1):
    print(f"  #{rank} {qid}: {freq} 次")

cursor.close()
conn.close()
print("\n✅ 离线分析完成！")
