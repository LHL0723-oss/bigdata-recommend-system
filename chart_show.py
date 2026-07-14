#!/usr/bin/env python3
# 生成热点题图表

import pymysql
import matplotlib.pyplot as plt

# 设置字体
plt.rcParams['font.sans-serif'] = ['DejaVu Sans', 'Arial Unicode MS', 'WenQuanYi Zen Hei', 'SimHei']
plt.rcParams['axes.unicode_minus'] = False

print("📊 正在连接数据库...")

try:
    conn = pymysql.connect(
        host='localhost',
        user='root',
        password='123456',  # ⚠️ 改成你的密码
        database='learning',
        charset='utf8mb4'
    )
    cursor = conn.cursor()
    
    # ⚠️ 关键修复：用 ranking 代替 rank
    cursor.execute("SELECT question_id, frequency FROM t_hot_questions ORDER BY ranking LIMIT 10")
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    
    if not data:
        print("❌ 表中没有数据")
        exit(1)
    
    questions = [row[0] for row in data]
    frequencies = [row[1] for row in data]
    
    print(f"✅ 读取到 {len(data)} 条数据")
    
    # 画图
    plt.figure(figsize=(10, 6))
    bars = plt.bar(questions, frequencies, color='steelblue', edgecolor='black')
    plt.xlabel('题目ID', fontsize=12)
    plt.ylabel('推荐次数', fontsize=12)
    plt.title('热点题 Top 10 推荐次数', fontsize=16)
    plt.xticks(rotation=45, ha='right')
    
    # 显示数值
    for bar, freq in zip(bars, frequencies):
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                 str(freq), ha='center', va='bottom', fontsize=11)
    
    plt.tight_layout()
    plt.savefig('hot_questions.png', dpi=150)
    print("✅ 图表已保存为 hot_questions.png")

except Exception as e:
    print(f"❌ 错误: {e}")
