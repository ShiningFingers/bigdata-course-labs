# 实验4：MapReduce 去重、求平均成绩、排序、取 Top5、分区实验 —— 学习笔记

> 本笔记根据实验报告 `20242093刘浩宇3.docx` 整理。报告原文以截图为主，
> 文中命令、输出均来自截图；**未在报告截图中出现的内容会明确标注"（原文未说明）"**，绝不编造。
> 注意：报告的 Java 源代码没有以截图形式出现，只有 Maven 编译输出（8 个源文件）和运行命令+结果。

---

## 1. 实验在做什么 / 最终产出什么

**一句话**：准备一份学生成绩数据（含重复行），用 5 个 MapReduce 程序分别完成 **去重、求平均分、按成绩排序、取成绩 Top5、按课号分区** 5 个任务，观察每个任务的 HDFS 输出。

**最终产出**：HDFS 上 5 个输出目录，每个目录里是对应任务的结果文件 `part-r-00000`。

---

## 2. 环境与前置条件

| 项目 | 值 |
|---|---|
| 操作系统 | Linux（用户名 `suzuha`，主机名 `lhy`） |
| Hadoop | 已部署，HDFS 可用 |
| 开发环境 | Windows 上 IDEA + Maven（项目名 `mr3`，包名 `com.suzuha.mr3`） |
| 服务器地址 | `192.168.88.129`（从 Windows 用 scp 传 jar 过去） |
| 数据文件 | `score.txt`，格式：`学号\t课号\t成绩`（Tab 分隔） |
| 原始数据量 | 21 条记录（含 1 条重复，去重后 20 条） |

**前置准备**：
- Hadoop 已启动（HDFS 可用）；
- 本地（Windows）用 Maven 把 `mr3` 项目打成 `jar-with-dependencies.jar`；
- 把 jar 传到 Linux 服务器。

---

## 3. 关键概念速览

| 术语 | 是什么 | 为什么需要 |
|---|---|---|
| **MapReduce** | Hadoop 的分布式计算框架，分 Map（映射）和 Reduce（归约）两阶段 | 把大任务拆成小块并行算，再合并结果 |
| **Mapper** | 映射阶段，把输入切分成 `<key, value>` 对输出 | 并行处理每条数据 |
| **Reducer** | 归约阶段，把相同 key 的 value 收集到一起聚合 | 得到最终结果 |
| **去重（Dedup）** | 利用 MapReduce 自动按 key 分组的特性，相同 key 只输出一条 | 去掉重复数据 |
| **Partitioner（分区器）** | 决定 Map 输出的每条数据送到哪个 Reduce | 实现按规则分文件输出（如按课号分 5 个文件） |
| **part-r-00000** | Reduce 输出文件，`r` 表示 Reduce 输出，数字是分区号 | 一个 Reduce 对应一个输出文件 |
| **Combiner** | 在 Map 端提前做局部聚合（可选） | 减少网络传输量，提升性能 |

---

## 4. 完整操作步骤

### Step 1：准备数据 score.txt

用 `echo -e`（`-e` 启用 `\t` 转义）逐行追加到文件：

```bash
cd ~
echo -e "001\t1\t98" > score.txt
echo -e "001\t3\t95" >> score.txt
echo -e "20242093\t2\t97" >> score.txt
echo -e "20242093\t2\t97" >> score.txt
echo -e "002\t1\t85" >> score.txt
echo -e "002\t5\t96" >> score.txt
echo -e "003\t2\t80" >> score.txt
echo -e "002\t2\t90" >> score.txt
echo -e "20242093\t1\t99" >> score.txt
echo -e "20242093\t3\t92" >> score.txt
echo -e "20242093\t4\t88" >> score.txt
echo -e "20242093\t5\t94" >> score.txt
echo -e "001\t2\t76" >> score.txt
echo -e "001\t4\t89" >> score.txt
echo -e "002\t3\t93" >> score.txt
echo -e "003\t3\t93" >> score.txt
echo -e "003\t1\t79" >> score.txt
echo -e "003\t5\t91" >> score.txt
echo -e "004\t2\t87" >> score.txt
echo -e "005\t3\t75" >> score.txt
```

> 注意第 3、4 行完全相同（`20242093\t2\t97`），这是故意制造的重复数据，用于验证去重。

验证数据（Tab 分隔符是否正确）：

```bash
cat -A score.txt | head -3
```

**预期输出**：
```
001^I1^I98$
001^I3^I95$
20242093^I2^I97$
```
`^I` 就是 Tab，`$` 是行尾。（对应报告 image2、image3）

统计行数：

```bash
wc -l score.txt
```

**预期输出**：`21 score.txt`

---

### Step 2：上传数据到 HDFS

```bash
hadoop fs -mkdir -p /mapreduce3_in
hadoop fs -put -f score.txt /mapreduce3_in/
hadoop fs -ls /mapreduce3_in
hadoop fs -cat /mapreduce3_in/score.txt | wc -l
```

**在干什么**：
- `-mkdir -p`：创建输入目录（`-p` 父目录不存在则一并创建）；
- `-put -f`：上传文件，`-f` 表示已存在则覆盖；
- `-ls`：确认文件上传成功；
- `-cat ... | wc -l`：确认 HDFS 上文件行数也是 21。

**预期输出**：
```
Found 1 items
-rw-r--r--   1 suzuha supergroup        219 2026-10-17 15:34 /mapreduce3_in/score.txt
21
```
（对应报告 image3）

---

### Step 3：Maven 编译打包（Windows 端）

在 Windows 的 IDEA 里对 `mr3` 项目执行 Maven 打包（`mvn package`）。

**预期输出**（对应报告 image4）：
```
[INFO] Compiling 8 source files to D:\desktop\mr3\target\classes
[INFO] Building jar: D:\desktop\mr3\target\mr3-1.0-SNAPSHOT.jar
[INFO] Building jar: D:\desktop\mr3\target\mr3-1.0-SNAPSHOT-jar-with-dependencies.jar
[INFO] BUILD SUCCESS
[INFO] Total time: 11.774 s
```

> 关键信息：项目共 **8 个 Java 源文件**，打出来两个 jar，其中 `jar-with-dependencies.jar`（带依赖）才是要上传运行的。

---

### Step 4：把 jar 传到 Linux 服务器

在 Windows PowerShell 里执行：

```powershell
scp D:\desktop\mr3\target\mr3-1.0-SNAPSHOT-jar-with-dependencies.jar suzuha@192.168.88.129:/home/suzuha/app3.jar
```

**在干什么**：用 `scp` 把带依赖的 jar 传到服务器，重命名为 `app3.jar`（方便后面调用）。

**预期输出**（对应报告 image5）：
```
mr3-1.0-SNAPSHOT-jar-with-dependencies.jar  100%  66MB  114.8MB/s  00:00
```

在 Linux 上验证：

```bash
ls -lh /home/suzuha/app3.jar
```

输出：`-rw-rw-r-- 1 suzuha suzuha 66M ... /home/suzuha/app3.jar`

---

### Step 5：运行去重任务 DedupJob

```bash
hadoop jar /home/suzuha/app3.jar com.suzuha.mr3.DedupJob /mapreduce3_in /mr3_dedup
```

**在干什么**：
- `hadoop jar`：提交 MapReduce 作业；
- `com.suzuha.mr3.DedupJob`：主类（去重作业）；
- `/mapreduce3_in`：输入目录；
- `/mr3_dedup`：输出目录（**必须不存在**，否则报错）。

查看结果：

```bash
hadoop fs -cat /mr3_dedup/part-r-00000
hadoop fs -cat /mr3_dedup/part-r-00000 | wc -l
```

**预期输出**（对应报告 image7）：
```
001    1    98
001    2    76
001    3    95
001    4    89
002    1    85
002    2    90
002    3    93
002    5    96
003    1    79
003    2    80
003    3    93
003    5    91
004    2    87
005    3    75
20242093    1    99
20242093    2    97
20242093    3    92
20242093    4    88
20242093    5    94
20
```

> 原始 21 条，去重后 **20 条**（去掉了一条重复的 `20242093 2 97`）。

---

### Step 6：运行求平均分任务 AvgJob

```bash
hadoop jar /home/suzuha/app3.jar com.suzuha.mr3.AvgJob /mapreduce3_in /mr3_avg
hadoop fs -cat /mr3_avg/part-r-00000
```

**在干什么**：按学号分组，计算每个学生所有课的平均成绩。

**预期输出**（对应报告 image8）：
```
001        89.50
002        91.00
003        83.33
004        85.00
005        71.50
20242093   94.00
```

> 例如 `001` 有 4 门课：98、95、76、89，平均 = (98+95+76+89)/4 = 89.5。

---

### Step 7：运行排序任务 SortJob

```bash
hadoop jar /home/suzuha/app3.jar com.suzuha.mr3.SortJob /mapreduce3_in /mr3_sort
hadoop fs -cat /mr3_sort/part-r-00000
```

**在干什么**：按成绩从小到大排序。

**预期输出**（对应报告 image9，升序）：
```
68    005    1
75    005    3
76    001    2
79    003    1
80    003    2
83    004    4
85    002    1
87    004    2
88    20242093    4
89    001    4
90    002    2
91    003    5
92    20242093    3
93    002    3
94    20242093    5
95    001    3
96    002    5
97    20242093    2
```

> 输出格式是 `成绩  学号  课号`，按成绩升序。

---

### Step 8：运行取 Top5 任务 Top5Job

```bash
hadoop jar /home/suzuha/app3.jar com.suzuha.mr3.Top5Job /mapreduce3_in /mr3_top5
hadoop fs -cat /mr3_top5/part-r-00000
```

**在干什么**：取成绩最高的前 5 条记录。

**预期输出**（对应报告 image10）：
```
99    20242093    1
98    001        1
97    20242093    2
96    002        5
95    001        3
```

> 成绩从高到低前 5 条。

---

### Step 9：运行分区任务 PartJob

```bash
hadoop jar /home/suzuha/app3.jar com.suzuha.mr3.PartJob /mapreduce3_in /mr3_part
hadoop fs -ls /mr3_part
```

**在干什么**：按课号分区——课号 1~5 分别输出到不同的 part 文件，所以有 5 个输出文件。

**预期输出**（对应报告 image11）：
```
Found 6 items
-rw-r--r--  ...  /mr3_part/_SUCCESS
-rw-r--r--  ...  /mr3_part/part-r-00000
-rw-r--r--  ...  /mr3_part/part-r-00001
-rw-r--r--  ...  /mr3_part/part-r-00002
-rw-r--r--  ...  /mr3_part/part-r-00003
-rw-r--r--  ...  /mr3_part/part-r-00004
```

> 5 个 `part-r-0000X` 文件分别对应课号 1~5。逐个查看：

```bash
echo "=== 课号 1 ==="; hadoop fs -cat /mr3_part/part-r-00000
echo "=== 课号 2 ==="; hadoop fs -cat /mr3_part/part-r-00001
echo "=== 课号 3 ==="; hadoop fs -cat /mr3_part/part-r-00002
echo "=== 课号 4 ==="; hadoop fs -cat /mr3_part/part-r-00003
echo "=== 课号 5 ==="; hadoop fs -cat /mr3_part/part-r-00004
```

**预期输出**（对应报告 image12、image13）：
```
=== 课号 1 ===
001        1    98
005        1    68
003        1    79
002        1    85
20242093   1    99

=== 课号 2 ===
003        2    80
20242093   2    97
004        2    87
002        2    90
001        2    76

=== 课号 3 ===
005        3    75
20242093   3    92
002        3    93
001        3    95

=== 课号 4 ===
20242093   4    88
001        4    89
004        4    83

=== 课号 5 ===
002        5    96
20242093   5    94
003        5    91
```

> 每个文件里是该课号的所有成绩记录。

---

## 5. 代码讲解

> ⚠️ **报告原文未贴出 Java 源代码**，只有 Maven 编译输出显示项目有 **8 个源文件**，主类都在 `com.suzuha.mr3` 包下。
> 以下是根据运行命令和输出反推的**各 Job 逻辑说明**，供理解用；具体代码以你自己的项目为准。

5 个 Job 的逻辑概览：

| Job 类 | 功能 | Map 输出 key | Map 输出 value | Reduce 逻辑 |
|---|---|---|---|---|
| `DedupJob` | 去重 | 整行文本（学号+课号+成绩） | 空/null | 相同 key 只输出一次 |
| `AvgJob` | 求每生平均分 | 学号 | 成绩 | 求该学号所有成绩的平均值 |
| `SortJob` | 按成绩排序 | 成绩 | 学号+课号 | 直接输出（靠 MR 自动排序） |
| `Top5Job` | 取成绩前 5 | 成绩（降序） | 学号+课号 | 只输出前 5 条 |
| `PartJob` | 按课号分区 | 课号 | 整行 | 自定义 Partitioner 按课号分到 5 个 Reduce |

**通用的 MR 程序结构**（每个 Job 都长这样）：

```java
// 主类里设置 Job
public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "JobName");
    job.setJarByClass(XxxJob.class);
    job.setMapperClass(XxxMapper.class);
    job.setReducerClass(XxxReducer.class);
    job.setOutputKeyClass(...);
    job.setOutputValueClass(...);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
}
```

**去重的核心思路**：把整行作为 Map 输出的 key，value 放空。MapReduce 框架会自动把相同 key 的 value 分到同一个 Reduce，Reduce 里只输出 key（不输出 value），就实现了去重。

**分区的核心思路**：继承 `Partitioner`，重写 `getPartition()` 方法，按课号返回 0~4，然后 `job.setNumReduceTasks(5)` 设置 5 个 Reduce，就会生成 5 个 part 文件。

> 如果你需要完整的 Java 代码，可参考同目录的 `MapReduce-WordCount实验-详细操作流程.md` 了解 MR 程序的完整写法，再按上表逻辑自行实现 5 个 Job。

---

## 6. 截图对照说明

| 报告截图 | 内容 | 转写要点 |
|---|---|---|
| image2 | 创建 score.txt | 21 条 echo -e 命令，含 1 条重复行 |
| image3 | 验证 Tab + 上传 HDFS | `cat -A` 看到 `^I`；`wc -l` = 21；`hadoop fs -put` |
| image4 | Maven 打包 | 8 source files，BUILD SUCCESS，生成 jar-with-dependencies |
| image5 | scp 传 jar | Windows→192.168.88.129，重命名 app3.jar |
| image6 | 运行 DedupJob | `hadoop jar ... com.suzuha.mr3.DedupJob` |
| image7 | 去重结果 | 20 行，少了一条重复 |
| image8 | 求平均结果 | 6 个学生的平均分 |
| image9 | 排序结果 | 按成绩升序 |
| image10 | Top5 结果 | 成绩最高 5 条 |
| image11 | 分区结果列表 | 5 个 part 文件 |
| image12 | 分区 1、2 内容 | 课号 1、2 的数据 |
| image13 | 分区 3、4、5 内容 | 课号 3、4、5 的数据 |

---

## 7. 常见报错与排查

### 原文提到过的报错

报告截图中**未出现明显报错**，所有任务都成功运行。

### 按环境差异推断的报错

1. **`Output directory hdfs://.../mr3_dedup already exists`**
   - 原因：输出目录已存在，MapReduce 不允许覆盖。
   - 解决：`hadoop fs -rm -r /mr3_dedup` 删除后再运行。

2. **`ClassNotFoundException: com.suzuha.mr3.DedupJob`**
   - 原因：传的 jar 不对（用了不带依赖的 jar），或主类名写错。
   - 解决：确保用 `jar-with-dependencies.jar`，主类名与代码一致。

3. **数据分隔符不是 Tab 导致解析错误**
   - 原因：`echo` 没加 `-e`，`\t` 没被转义成真正的 Tab。
   - 解决：用 `cat -A` 检查，看到 `^I` 才是正确的 Tab。

4. **`hadoop jar` 命令被截断显示**
   - 报告 image6 里命令行太长被折行，实际完整命令是：
     `hadoop jar /home/suzuha/app3.jar com.suzuha.mr3.DedupJob /mapreduce3_in /mr3_dedup`

---

## 8. 自检清单

| 检查项 | 命令 / 方法 | 成功标志 |
|---|---|---|
| 数据文件行数正确 | `wc -l score.txt` | 21 |
| Tab 分隔符正确 | `cat -A score.txt \| head -1` | 有 `^I` |
| HDFS 上传成功 | `hadoop fs -ls /mapreduce3_in` | 有 score.txt |
| jar 上传成功 | `ls -lh /home/suzuha/app3.jar` | 约 66M |
| 去重成功 | `hadoop fs -cat /mr3_dedup/part-r-00000 \| wc -l` | 20 |
| 平均分正确 | `cat /mr3_avg/part-r-00000` | 001=89.50 等 |
| 排序正确 | `cat /mr3_sort/part-r-00000` | 成绩升序 |
| Top5 正确 | `cat /mr3_top5/part-r-00000` | 5 条，成绩递减 |
| 分区正确 | `hadoop fs -ls /mr3_part` | 5 个 part-r 文件 |

---

## 9. 小结 + 易忘命令速记

**一句话小结**：MapReduce 靠 `<key, value>` 分组来实现各种数据处理——去重靠 key 去重、平均分靠按学号分组求和、排序靠 key 自动排序、Top5 靠限制输出条数、分区靠自定义 Partitioner + 多 Reduce。

**易忘命令速记**：
```bash
# 数据准备
echo -e "学号\t课号\t成绩" >> score.txt     # -e 让 \t 生效
cat -A score.txt | head -3                  # 检查 Tab 是否正确
wc -l score.txt                              # 统计行数

# HDFS 操作
hadoop fs -mkdir -p /mapreduce3_in
hadoop fs -put -f score.txt /mapreduce3_in/
hadoop fs -ls /mapreduce3_in
hadoop fs -cat /mapreduce3_in/score.txt | wc -l
hadoop fs -rm -r /mr3_xxx                   # 删输出目录（重跑前必做）

# 运行 MR 作业
hadoop jar /home/suzuha/app3.jar com.suzuha.mr3.<Job名> <输入目录> <输出目录>

# 查看结果
hadoop fs -cat /mr3_xxx/part-r-00000
hadoop fs -cat /mr3_xxx/part-r-00000 | wc -l
```
