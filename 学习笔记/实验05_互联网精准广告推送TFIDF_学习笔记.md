# 实验5：互联网精准广告推送算法（MapReduce 实现 TF-IDF）学习笔记

> 源报告：`20242093刘浩宇5.docx`（实验序号：5，主题：互联网精准广告推送算法 / MapReduce 实现 TF-IDF）

---

## 1. 这个实验在做什么

用 **TF-IDF 算法**给 18 条微博中的每个关键词算"权重"，权重越高说明这个词越能代表该用户。最终产出一张"微博用户名 → 词:权重"的表，从而从混杂微博里识别出对华为手机感兴趣的目标用户（广告推送对象）。整个算法用 **三个串联的 MapReduce 作业**实现。

---

## 2. 环境与前置条件

| 项 | 内容 |
|---|---|
| 操作系统 | Linux（虚拟机，4 GB 内存、2 核） |
| JDK | Java 11 |
| Hadoop | 已安装并能启动（HDFS + YARN） |
| 构建工具 | Maven（`mvn` 命令可用） |
| 分词器 | IKAnalyzer（中文分词，作为项目依赖） |
| Maven 工程 | groupId = `com.lhy.tj`，artifactId = `tfidf`，主类 `com.lhy.tj.TFIDFJobs`，编译级别 Java 11 |
| 数据 | `weibo.txt`（18 条微博，每行格式：`用户名\t微博内容`），需放在本地 |
| 前置启动 | HDFS 与 YARN 必须先启动 |

> 原文未说明 Hadoop 的具体版本和 IKAnalyzer 的 Maven 坐标，需参照你自己的环境。

---

## 3. 关键概念速览

- **TF（Term Frequency，词频）**：某个词在某条微博里出现的次数。出现越多，说明用户越关注这个词。
- **DF（Document Frequency，文档频次）**：某个词在多少条微博里出现过。出现的微博越多，说明这个词越"烂大街"，区分度越低。
- **IDF（Inverse Document Frequency，逆文档频次）**：`IDF = ln(N / DF)`，N 是微博总条数。DF 越大，IDF 越小，用来压低"普遍词"的权重。
- **TF-IDF 权重**：`W = TF × IDF = TF × ln(N / DF)`。**高频 × 稀有 = 高代表性**，这就是 TF-IDF 的核心思想。
- **MapReduce 作业串联**：一个复杂算法拆成多个 Job，前一个 Job 的输出作为后一个 Job 的输入，每步都有中间产物便于调试。
- **分布式缓存 `addCacheFile`**：把小文件（如 DF 表）分发到所有计算节点，在 Mapper 的 `setup()` 里加载到内存，避免在 Reduce 端做 Join。
- **N（总条数）**：本实验 N = 18，即微博总条数。

---

## 4. 完整操作步骤

### Step 1：启动 Hadoop（若未启动）

```bash
start-dfs.sh
start-yarn.sh
```

**说明**：`start-dfs.sh` 启动 HDFS（NameNode、DataNode），`start-yarn.sh` 启动 YARN（ResourceManager、NodeManager）。MapReduce 作业需要 YARN 来调度。

**正常现象**：`jps` 命令能看到 NameNode、DataNode、ResourceManager、NodeManager 进程。

### Step 2：上传原始数据到 HDFS

```bash
hdfs dfs -put weibo.txt /tj/input/
```

**说明**：把本地的 `weibo.txt` 上传到 HDFS 的 `/tj/input/` 目录。如果目录不存在需先 `hdfs dfs -mkdir -p /tj/input`。

**正常现象**：`hdfs dfs -ls /tj/input/` 能看到 `weibo.txt`。

### Step 3：清理历史输出（否则报错）

```bash
hdfs dfs -rm -r /tj/output1 /tj/output2 /tj/output3
```

**说明**：MapReduce 要求输出目录**不能已存在**，否则报 `Output directory already exists`。每次重跑前必须删掉三个作业的输出目录。

**正常现象**：显示 `Deleted /tj/output1` 等。

### Step 4：打包并提交三次串联作业

```bash
mvn clean package -DskipTests
hadoop jar ~/tfidf-1.0-SNAPSHOT-jar-with-dependencies.jar 3
```

**说明**：
- `mvn clean package -DskipTests`：用 Maven 打包，`-DskipTests` 跳过单元测试，生成带依赖的 fat jar（`jar-with-dependencies`）。
- `hadoop jar ... 3`：提交作业，参数 `3` 表示依次跑 3 个 Job（Job1→Job2→Job3）。

**正常现象**：控制台依次打印三个 Job 的进度（map 0%→100%，reduce 0%→100%），最后输出 `ALL DONE`。

### Step 5：查看最终结果

```bash
hdfs dfs -cat /tj/output3/part-r-00000
```

**说明**：查看 Job3 的最终输出，即每个用户的关键词权重表。

**正常现象**：输出格式为 `用户名    词1:权重1  词2:权重2  ...`，例如：
```
20242093    hadoop:2.890  p10:2.562  华为:2.833  拍照:5.375  学号:8.671  ...
```

---

## 5. 代码讲解

整个工程只有一个主类 `TFIDFJobs`，里面定义了 3 个 Mapper、3 个 Reducer 和 `main` 方法。`main` 按顺序提交 3 个 Job。

### 5.1 整体流程

```
原始微博(18条)
    │
    ├─ Job1: 统计每条微博的词频 TF，并输出总条数 N
    │        输出: 词_ID \t TF      +  count \t 18
    ▼
    ├─ Job2: 去掉用户维度，统计每个词的 DF
    │        输出: 词 \t DF（共166个词）
    ▼
    └─ Job3: 读 DF 缓存，计算 W = TF × ln(18/DF)，按用户拼接
             输出: 微博用户名 \t 词:权重 词:权重 ...
```

### 5.2 Job1：FirstMapper —— 分词 + 输出词频

```java
public static class FirstMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable ONE = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context ctx) {
        String[] parts = value.toString().split("\t");
        if (parts.length < 2) return;
        String id = parts[0].trim();       // 用户名
        String content = parts[1];          // 微博内容
        for (String w : seg(content)) {     // IKAnalyzer 分词
            ctx.write(new Text(w + "_" + id), ONE);  // 输出 "词_用户名" → 1
        }
        ctx.write(new Text("count"), ONE);  // 每条微博贡献一个 count，用于统计总条数 N
    }
}
```

**讲解**：
- 输入每行是 `用户名\t微博内容`，按 `\t` 切开。
- 用 IKAnalyzer 对微博内容分词，每个词输出 `(词_用户名, 1)`。把词和用户名拼在一起作为 key，这样 Reduce 时能按"某用户的某词"聚合。
- 同时输出一个 `(count, 1)`，每条微博输出一次，Reduce 后累加得到总条数 N。

### 5.3 Job2：SecondMapper —— 去掉用户名，只留词

```java
public static class SecondMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable ONE = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context ctx) {
        String[] parts = value.toString().split("\t");
        if (parts.length < 2) return;
        String k = parts[0].trim();
        if ("count".equals(k)) return;          // 跳过全局计数行
        int idx = k.lastIndexOf("_");
        if (idx <= 0) return;
        ctx.write(new Text(k.substring(0, idx)), ONE);  // 只取词部分，丢掉用户名
    }
}
```

**讲解**：
- Job1 输出格式是 `词_用户名 \t TF`。这里用 `lastIndexOf("_")` 切出词，丢掉用户名。
- 跳过 `count` 行（那是总条数，不是词）。
- 输出 `(词, 1)`，Reduce 后累加得到该词出现在多少条微博中，即 DF。

### 5.4 Job3：ThirdMapper —— 读 DF 缓存，算权重（核心 + 最容易踩坑）

```java
public static class ThirdMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final Map<String, Integer> dfMap = new HashMap<>();
    private int N = 1;

    @Override
    protected void setup(Context ctx) throws IOException {
        N = ctx.getConfiguration().getInt("tj.N", 1);
        URI[] cacheFiles = ctx.getCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            String localName = new Path(cacheFiles[0]).getName();
            // 【关键】必须显式指定 UTF-8，否则中文 DF 读成乱码
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(new FileInputStream(localName), StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] p = line.split("\t");
                    dfMap.put(p[0], Integer.parseInt(p[1]));
                }
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context ctx) throws IOException {
        String[] parts = value.toString().split("\t");
        if (parts.length < 2) return;
        String k = parts[0].trim();
        if ("count".equals(k)) return;
        int idx = k.lastIndexOf("_");
        if (idx <= 0) return;
        String word = k.substring(0, idx);
        String id = k.substring(idx + 1);
        int tf = Integer.parseInt(parts[1]);
        int df = dfMap.getOrDefault(word, 1);   // 读不到的词默认 DF=1
        double w = tf * Math.log((double) N / df);
        ctx.write(new Text(id), new Text(word + ":" + String.format("%.3f", w)));
    }
}
```

**讲解**：
- `setup()` 在 Map 任务开始时执行一次。它从分布式缓存读取 Job2 输出的 DF 表（`part-r-00000`），加载到内存 `dfMap`。
- `map()` 处理 Job1 的输出（`词_用户名 \t TF`），取出词、用户名、TF，查 DF，算 `W = TF × ln(N/DF)`。
- 输出 `(用户名, 词:权重)`，Reducer 把同一用户的所有词拼到一行。

> **重中之重**：读 DF 文件必须用 `InputStreamReader(..., StandardCharsets.UTF_8)`，不能用 `FileReader`。`FileReader` 用系统默认编码，在分布式节点上中文会变成乱码，导致 `dfMap.getOrDefault` 永远返回默认值 1，IDF 失效，权重退化成纯 TF。

### 5.5 main 方法：串联三个 Job

```java
public static void main(String[] args) throws Exception {
    // OUT1, OUT2, OUT3 分别是三个作业的 HDFS 输出路径
    // Job1: 输入 /tj/input，输出 /tj/output1
    // Job2: 输入 /tj/output1，输出 /tj/output2
    // Job3: 输入 /tj/output1，缓存 /tj/output2/part-r-00000，输出 /tj/output3
    // Job3 通过 job.addCacheFile(OUT2 + "/part-r-00000") 把 DF 表分发到各节点
}
```

**讲解**：三个 Job 顺序执行，Job2 和 Job3 都依赖 Job1 的输出，Job3 还依赖 Job2 的输出（作为缓存）。

---

## 6. 截图对照说明

| 图号 | 内容 | 文字转写 |
|---|---|---|
| 图1 | 原始微博数据集 | 18 条微博，每行 `用户名\t内容`，混合华为手机相关微博与无关生活微博 |
| 图2 | 核心 MapReduce 代码 | `TFIDFJobs.java`，含 FirstMapper/SecondMapper/ThirdMapper 及对应 Reducer，完整代码较长贴在文档末尾 |
| 图3 | Job1 输出（词频 TF） | 格式 `词_ID \t TF`，如 `20242093_20242093  3` 表示"20242093"在该用户3条微博中各出现1次，TF=3 |
| 图4 | Job2 输出（DF 最高15词） | 手机 7、华为 7、不错 6、p10 5、真的 4、还是 4、效果 3、摄像头 3、拍照 3… 另 `count 18` 为总条数 |
| 图5 | Job3 最终输出 | 15 个用户的关键词权重表，每行 `用户名  词:权重 词:权重 ...` |
| 图6 | 学号与姓名所在微博的权重（重点图） | `hdfs dfs -cat /tj/output3/part-r-00000 \| grep -E "刘浩宇|20242093"` 的输出，见下方 |

**图6 重点结果**（20242093 与 刘浩宇 的关键词权重）：
```
20242093    hadoop:2.890  p10:2.562  wordcount:2.890  不过:2.197  数据:2.890  在学:2.890
            徕卡:2.890  效果:1.792  替代:2.890  那一刻:2.890  这款:2.890  华为:2.833
            卡片机:2.890  可以:4.394  还是:1.504  喜欢:2.890  20242093:8.671  赢了:2.890
            虚化:2.890  能力强:2.890  考虑:2.197  真的:1.504  特别:2.197  有点:2.890
            最近:2.197  摄像头:3.584  拍照:5.375  手机:2.833  开心:2.890  已经:2.890
            学号:8.671  夜景:2.890  同学:2.890  人像:2.890  mapreduce:2.890

刘浩宇      说说:2.890  不错:1.099  华为:3.778  可以:2.197  的人:2.197  有没有:2.197
            真的:1.504  p10:2.562  考虑:4.394  续航:2.890  比上:2.890  朋友:5.781
            摄像头:1.792  拍照:3.584  手机:3.778  一代:2.890  入手:2.890  推荐:5.781
            太多:2.197  惊艳:2.890  值不值得:2.890  用过:2.197  调校:2.890  效果:1.792
```

**数据量变化**：18 条 → 260 个(词,1) → 213 条词频 → 166 个 DF → 15 个用户结果。

---

## 7. 常见报错与排查

### 原文提到过的问题

**问题一：Job3 中文词 DF 读取乱码，IDF 失效（主要问题）**

- **现象**：所有中文词的权重都是 `2.890` 的整数倍（`1×ln(18)`, `2×ln(18)`…），即 DF 被当成 1。只有英文数字词 `p10` 正确（DF=5）。
- **原因**：`ThirdMapper.setup()` 用了 `new BufferedReader(new FileReader(localName))`，`FileReader` 采用系统默认字符集，中文 key 解码成乱码，`dfMap.getOrDefault(word, 1)` 查不到，全部返回默认 1。
- **解决**：改用显式 UTF-8：
  ```java
  BufferedReader br = new BufferedReader(
      new InputStreamReader(new FileInputStream(localName), StandardCharsets.UTF_8));
  ```
  并新增 `import java.io.FileInputStream;` 和 `import java.nio.charset.StandardCharsets;`。
- **校验**：在 `setup()` 末尾打印 `System.out.println("DF loaded: " + dfMap.size() + " , N = " + N);`

**问题二：YARN 内存超卖，任务卡在 0%**

- **现象**：作业进度长期停在 `map 0% reduce 0%`，或容器被 kill。
- **原因**：虚拟机仅 4 GB 内存、2 核，YARN 默认资源配置超出物理上限。
- **解决**：修改 YARN 配置文件（XML）。注意：主类未实现 `ToolRunner`，命令行 `-D` 参数不生效，必须改 XML 配置文件。
- （原文未给出具体修改了哪些配置项的数值，标"原文未说明"）

**问题三：IKAnalyzer 对中文人名识别弱**

- **现象**："刘浩宇"被切成单字，因词长过滤（≥2）未进入关键词。
- **处理**：属于分词工具固有特性，非代码错误，如实保留结果。"刘浩宇"作为用户名出现在行首。

### 按环境差异推断的排查方向

- **`Output directory already exists`**：重跑前忘记删输出目录，执行 Step 3 的 `rm -r`。
- **`ClassNotFoundException` / 依赖缺失**：Maven 打包时未生成 `jar-with-dependencies`，检查 `pom.xml` 是否配置了 `maven-assembly-plugin`。
- **中文分词结果为空**：检查 IKAnalyzer 依赖是否正确引入，词典配置是否到位。

---

## 8. 自检清单

- [ ] `jps` 能看到 NameNode、DataNode、ResourceManager、NodeManager
- [ ] `hdfs dfs -ls /tj/input/` 能看到 `weibo.txt`
- [ ] `hdfs dfs -cat /tj/output1/part-r-00003` 输出 `count  18`（总条数）
- [ ] `hdfs dfs -cat /tj/output2/part-r-00000 | head` 能看到 `词  DF` 格式，DF 值不全是 1
- [ ] `hdfs dfs -cat /tj/output3/part-r-00000` 输出格式为 `用户名  词:权重 ...`
- [ ] 校验"华为"权重：DF=7，`ln(18/7)≈0.944`，TF=1 时权重应为 0.944 左右
- [ ] 校验"学号"权重：DF=1，`ln(18/1)=2.890`，TF=3 时权重应为 8.671（`3×2.890`）

---

## 9. 小结与易忘命令速记

**一句话小结**：TF-IDF = TF × ln(N/DF)，用三个 MapReduce 作业分别算 TF、DF、最终权重，核心坑是读分布式缓存时必须显式指定 UTF-8。

**易忘命令速记**：
```bash
start-dfs.sh && start-yarn.sh          # 启动 Hadoop
hdfs dfs -put weibo.txt /tj/input/     # 上传数据
hdfs dfs -rm -r /tj/output1 /tj/output2 /tj/output3   # 清理输出
mvn clean package -DskipTests          # 打包
hadoop jar ~/tfidf-1.0-SNAPSHOT-jar-with-dependencies.jar 3   # 跑3个Job
hdfs dfs -cat /tj/output3/part-r-00000 | grep -E "刘浩宇|20242093"  # 查结果
```
