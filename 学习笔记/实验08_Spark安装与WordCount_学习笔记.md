# 实验8：Spark 安装与 WordCount —— 学习笔记

## 1. 一句话说明这个实验在做什么

在单机上以 **Local 模式**安装 Spark 3.5.9，启动 `spark-shell`，用 **Scala** 写 4 行 RDD 算子完成单词计数（WordCount），对结果排序并持久化到本地文件，最终产出词频统计结果。

## 2. 环境与前置条件

| 项 | 值 | 说明 |
|---|---|---|
| Spark 版本 | 3.5.9 | 选用 bin-hadoop3 预编译包，匹配 Hadoop 3.4.2 |
| Hadoop 版本 | 3.4.2 | 已安装，Spark 复用其环境 |
| JDK 版本 | 11.0.2 | Spark 3.5.9 与 JDK 11 兼容；Spark 4.x 要求更高 JDK，故不选 4.x |
| Scala 版本 | 2.12.18 | spark-shell 启动时显示 |
| 运行模式 | Local local[2] | 2 个工作线程，单机多线程模拟分布式 |
| 操作系统用户 | suzuha | |
| Spark Web UI 端口 | 4040 | 截图显示地址 `http://192.168.88.129:4040` |
| 数据文件路径 | /home/suzuha/sparkdata/word.txt | |

**前置准备**：
- Hadoop 已安装并可正常使用（Spark 复用 Hadoop 环境变量，如 `HADOOP_HOME`）
- JDK 11 已安装
- 安装包：`spark-3.5.9-bin-hadoop3.tgz`（具体文件名原文未给出，按版本推断）

## 3. 关键概念速览

- **RDD**（Resilient Distributed Dataset，弹性分布式数据集）：Spark 的核心数据抽象，一个可分区、可并行计算的不可变集合。
- **转换算子**（Transformation）：对 RDD 做变换返回新 RDD，**lazy（延迟）执行**，只记录计算逻辑不真正跑。如 `flatMap`、`map`、`reduceByKey`、`sortBy`。
- **行动算子**（Action）：触发真正的计算并返回结果。如 `collect()`、`saveAsTextFile()`、`count()`。
- **延迟计算**（Lazy Evaluation）：Spark 遇到转换算子不立即执行，直到遇到行动算子才把整个计算链路提交执行。这就是"敲了半天没反应"的原因。
- **分区**（Partition）：RDD 的逻辑分片，决定并行度。`local[2]` 有 2 个分区，保存结果时就生成 2 个 `part-*` 文件。
- **Master URL**：`local` 单线程、`local[K]` K 个线程、`local[*]` 按 CPU 核数。本实验用 `local[2]`。
- **shuffle**：`reduceByKey` 会触发 shuffle（不同分区的数据按 key 重新分发），是 Spark 中最耗时的操作。

## 4. 完整操作步骤

### Step 1：安装 Spark（Local 模式）

> ⚠️ 报告中"安装过程记录"一节**只有标题，未给出具体解压/配置命令**（原文未说明）。以下按实验方法部分的描述整理：

```bash
# 1. 解压安装包到 /usr/local（具体路径原文未说明，按惯例推断）
tar -zxf spark-3.5.9-bin-hadoop3.tgz -C /usr/local/

# 2. 配置环境变量（在 ~/.bashrc 或 /etc/profile 中添加）
export SPARK_HOME=/usr/local/spark-3.5.9-bin-hadoop3
export PATH=$PATH:$SPARK_HOME/bin

# 3. 使配置生效
source ~/.bashrc

# 4. 验证配置生效
echo $SPARK_HOME
which spark-shell
```

- 为什么选 Spark 3.5.9 而不是 4.x：本机 JDK 是 11.0.2，Spark 4.x 要求更高 JDK 版本，会启动失败。3.5.9 与 JDK 11 兼容。
- 为什么选 bin-hadoop3 包：与本机已装的 Hadoop 3.4.2 匹配，避免额外编译或替换依赖。
- 为什么用 Local 模式：单节点多线程，无需部署其他节点，适合教学/调试，正符合单机 4 GB 内存环境。

### Step 2：准备测试数据

```bash
mkdir -p /home/suzuha/sparkdata
cat > /home/suzuha/sparkdata/word.txt << 'EOF'
20242093 刘浩宇
hello spark
hello scala
hello world
spark scala
20242093 刘浩宇
spark hadoop
20242093 刘浩宇 大数据系统基础
EOF
```

- 干什么：创建 8 行空格分隔的文本。学号 `20242093` 和姓名 `刘浩宇` 各出现 3 次，统计后会以 `(20242093,3)`、`(刘浩宇,3)` 出现在结果里，作为本人完成实验的身份标识。
- 为什么用空格分隔：WordCount 分词逻辑是 `split(" ")`，用空格分隔才能与代码一致（这与 Hive 实验里用逗号指定列分隔符不是一回事）。
- 验证：`cat /home/suzuha/sparkdata/word.txt` 应输出上述 8 行。

### Step 3：启动 spark-shell

```bash
spark-shell --master local[2]
```

- 预期输出（关键信息）：
  ```
  Setting default log level to "WARN".
  Spark context Web UI available at http://192.168.88.129:4040
  Spark context available as 'sc' (master = local[2], app id = local-1793335286349).
  Spark session available as 'spark'.
  Welcome to
        ____              __
       / __/__  ___ _____/ /__
      _\ \/ _ \/ _ `/ __/  '_/
     /___/ .__/\_,_/_/ /_/\_\   version 3.5.9
        /_/
  Using Scala version 2.12.18 (Java HotSpot(TM) 64-Bit Server VM, Java 11.0.2)
  scala>
  ```
- 启动后已自动创建：
  - `sc`：SparkContext（上下文），等效于 `new SparkConf().setAppName("Spark shell").setMaster("local[2]")` + `new SparkContext(conf)`
  - `spark`：SparkSession
- `--master local[2]`：以 2 个工作线程在本机运行。`local` 单线程、`local[K]` K 线程、`local[*]` 按 CPU 核数。
- 截图中有一条 WARN：`NativeCodeLoader: Unable to load native-hadoop library`，是正常警告，不影响功能。

### Step 4：用 Scala 完成单词计数

在 `scala>` 提示符下依次输入：

```scala
// 1. 从本地文件创建 RDD（每行是一个 String）
val lines = sc.textFile("file:///home/suzuha/sparkdata/word.txt")

// 2. 分词 → 映射 → 归并（均为转换算子，不立即执行）
val wordCount = lines
  .flatMap(line => line.split(" "))      // 按空格切分并拍扁成单词流
  .map(word => (word, 1))                // 每个单词映射为 (单词, 1)
  .reduceByKey((a, b) => a + b)          // 相同单词的计数相加

// 3. 行动算子：真正触发计算
wordCount.collect()
```

- **预期输出**：
  ```
  res0: Array[(String, Int)] = Array((scala,2), (刘浩宇,3), (20242093,3), (hello,3), (world,1), (spark,3), (大数据系统基础,1), (hadoop,1))
  ```
- **为什么敲完前三行没结果**：`flatMap`、`map`、`reduceByKey` 都是转换算子，只记录逻辑，直到 `collect()`（行动算子）才真正提交作业。
- **路径注意**：本地文件 URI 是三个斜杠 `file:///`，不是两个。这是从课件到实践最容易出错的一处。

**四条语句逐行解释**：

| 语句 | 类型 | 作用 |
|---|---|---|
| `sc.textFile(...)` | 转换 | 读取文件，每行一个元素，得到 `RDD[String]` |
| `flatMap(line => line.split(" "))` | 转换 | 把每行按空格切分成单词数组，再"拍扁"成单词流 |
| `map(word => (word, 1))` | 转换 | 每个单词变成 `(单词, 1)` 键值对 |
| `reduceByKey((a,b) => a+b)` | 转换 | 按 key 分组，把 value 累加 |
| `collect()` | 行动 | 触发计算，把结果收集到驱动端返回数组 |

### Step 5：结果排序与持久化

```scala
// 按词频降序输出（false = 降序）
wordCount.sortBy(_._2, false).collect()

// 保存到本地目录
wordCount.sortBy(_._2, false).saveAsTextFile("file:///home/suzuha/sparkdata/output")
```

- **排序预期输出**：
  ```
  res1: Array[(String, Int)] = Array((刘浩宇,3), (20242093,3), (hello,3), (spark,3), (scala,2), (world,1), (大数据系统基础,1), (hadoop,1))
  ```
- `sortBy(_._2, false)`：`_._2` 取元组第二个元素（即计数），`false` 表示降序。
- `saveAsTextFile` 保存的是排序后的 RDD。

### Step 6：退出 spark-shell 并核对结果文件

```scala
:quit
```

回到 Linux 终端：

```bash
ls ~/sparkdata/output
cat ~/sparkdata/output/part-*
```

- **预期输出**：
  ```
  part-00000  part-00001  _SUCCESS
  ```
  ```
  (刘浩宇,3)
  (20242093,3)
  (hello,3)
  (spark,3)
  (scala,2)
  (world,1)
  (大数据系统基础,1)
  (hadoop,1)
  ```
- **为什么是两个 part 文件**：`local[2]` 有 2 个分区，输出文件数 = 分区数。`_SUCCESS` 是作业成功标记文件。课件示例中单分区保存就只有 `part-00000` 一个。

## 5. 代码讲解

完整 Scala 代码（附录 C）：

```scala
// 1. 从本地文件创建 RDD
val lines = sc.textFile("file:///home/suzuha/sparkdata/word.txt")

// 2. 分词、计数、归并（均为转换算子，不立即执行）
val wordCount = lines
  .flatMap(line => line.split(" "))     // 按空格切分并拍扁
  .map(word => (word, 1))               // 每个单词映射为 (单词, 1)
  .reduceByKey((a, b) => a + b)         // 相同单词的计数相加

// 3. 行动算子：真正触发计算
wordCount.collect()

// 4. 按词频降序输出
wordCount.sortBy(_._2, false).collect()

// 5. 结果持久化到本地目录
wordCount.sortBy(_._2, false).saveAsTextFile("file:///home/suzuha/sparkdata/output")
```

**算子链式调用的数据流**：

```
word.txt (8行文本)
  │  sc.textFile
  ▼
lines: RDD[String]            // ["20242093 刘浩宇", "hello spark", ...]
  │  flatMap(_.split(" "))
  ▼
RDD[String]                   // ["20242093","刘浩宇","hello","spark",...]  (共18个单词)
  │  map(word => (word, 1))
  ▼
RDD[(String, Int)]            // [("20242093",1),("刘浩宇",1),("hello",1),...]
  │  reduceByKey(_+_)
  ▼
wordCount: RDD[(String, Int)] // [("20242093",3),("刘浩宇",3),("hello",3),...]
  │  sortBy(_._2, false)
  ▼
RDD[(String, Int)]            // 按计数降序排列
  │  collect() / saveAsTextFile()
  ▼
结果
```

**自洽验证**：源文件共 18 个单词，结果中各词次数之和 = 3+3+3+3+2+1+1+1 = 18，闭合。

## 6. 截图对照说明

| 报告截图 | 内容转写 |
|---|---|
| 图2（spark-shell 启动） | 命令 `spark-shell --master local[2]`；Web UI `http://192.168.88.129:4040`；`sc` (master=local[2])、`spark` 已就绪；Spark **3.5.9**；Scala **2.12.18**；Java **11.0.2**；有 `NativeCodeLoader` WARN（正常）。 |
| 图3（数据文件） | `cat /home/suzuha/sparkdata/word.txt` 输出 8 行空格分隔文本，含 `20242093 刘浩宇` 等。 |
| 图4（词频统计执行） | 三条 Scala 语句依次执行，`wordCount.collect()` 输出 `res0: Array[(String, Int)] = Array((scala,2), (刘浩宇,3), (20242093,3), (hello,3), (world,1), (spark,3), (大数据系统基础,1), (hadoop,1))`。 |
| 图5（排序与保存） | `wordCount.sortBy(_._2, false).collect()` 输出 `res1: Array(...)` 降序排列；`saveAsTextFile("file:///home/suzuha/sparkdata/output")`；`:quit` 退出；`ls ~/sparkdata/output` 显示 `part-00000  part-00001  _SUCCESS`。 |
| 图6（核对结果文件） | `cat ~/sparkdata/output/part-*` 输出 8 行 `(单词,次数)`，与屏幕降序输出一致。 |

> 注：报告中 image1 为校徽装饰图，不含操作信息，略。

## 7. 常见报错与排查

### 原文提到过的

| 问题 | 根因 | 解决 |
|---|---|---|
| 敲完 `flatMap`/`map`/`reduceByKey` 后没结果 | 这三个都是转换算子，Spark 延迟计算，不触发执行 | 敲 `collect()` 等行动算子才会真正计算 |

### 按环境差异推断的

| 问题 | 现象 | 处理 |
|---|---|---|
| 路径写成 `file://` 而不是 `file:///` | `textFile` 找不到文件 | 本地文件 URI 必须三个斜杠：`file:///home/...` |
| `NativeCodeLoader` WARN | 启动时警告无法加载 native-hadoop 库 | 不影响功能，可忽略 |
| `saveAsTextFile` 报目录已存在 | 输出目录已存在 | 先 `rm -rf` 输出目录再保存 |
| `spark-shell` 启动失败 | 可能 JDK 版本不匹配或 `SPARK_HOME` 未配置 | 确认 JDK 11、`echo $SPARK_HOME` 有值、`which spark-shell` 能找到 |
| 结果只有 `part-00000` 一个文件 | 并行度为 1（`local` 或单分区） | 用 `local[2]` 或重分区会生成多个 part 文件（属正常现象，非错误） |

## 8. 自检清单

- [ ] `echo $SPARK_HOME` 输出 Spark 安装路径
- [ ] `which spark-shell` 能找到命令
- [ ] `spark-shell --master local[2]` 启动后提示符为 `scala>`
- [ ] 启动信息显示 Spark 3.5.9、Scala 2.12.18、Java 11.0.2
- [ ] `/home/suzuha/sparkdata/word.txt` 存在，8 行
- [ ] `wordCount.collect()` 返回 8 个不同单词
- [ ] 结果中 `(20242093,3)` 和 `(刘浩宇,3)` 存在（身份标识）
- [ ] 各词次数之和 = 18（与源文件单词总数一致）
- [ ] 排序后结果按次数降序：3 次词在前，2 次居中，1 次在后
- [ ] `~/sparkdata/output/` 下有 `part-00000`、`part-00001`、`_SUCCESS` 三个文件
- [ ] `cat part-*` 的内容与屏幕排序输出完全一致

## 9. 小结 + 易忘命令速记

**小结**：本实验用 Spark Local 模式 4 行 Scala 代码完成了 WordCount，核心是理解 **转换算子（lazy）与行动算子（触发）的边界**。`flatMap → map → reduceByKey` 只是记录计算逻辑，`collect()` 才真正执行。`local[2]` 的 2 个分区在保存时生成 2 个 `part-*` 文件，是并行度最直观的证据。与 MapReduce 相比，Spark 基于内存、API 更简洁，开发效率更高。

**易忘命令速记**：

```bash
# 启动 spark-shell
spark-shell --master local[2]

# Scala 词频统计（在 scala> 下）
val lines = sc.textFile("file:///home/suzuha/sparkdata/word.txt")
val wordCount = lines.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
wordCount.collect()

# 降序排序
wordCount.sortBy(_._2, false).collect()

# 保存结果
wordCount.sortBy(_._2, false).saveAsTextFile("file:///home/suzuha/sparkdata/output")

# 退出
:quit

# 核对结果
ls ~/sparkdata/output
cat ~/sparkdata/output/part-*
```

**与实验7（Hive JDBC）的联系**：两者共用同一台机器的 Hadoop 环境——Hadoop 负责存储管理，Hive/Spark 负责计算。Hive 把 SQL 翻译成 MapReduce（偏重数据仓库查询），Spark 直接用 RDD 算子在内存计算（更适合迭代与交互式分析）。
