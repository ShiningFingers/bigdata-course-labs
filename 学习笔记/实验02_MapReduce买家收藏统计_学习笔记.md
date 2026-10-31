# 实验2：MapReduce WordCount（买家收藏统计）— 学习笔记

> 面向零基础，照着命令做就能跑通。本文根据实验报告原文转写，命令/路径/输出均来自原文，未出现的细节标注"（原文未说明）"。

---

## 1. 这个实验在做什么

用 Java 写一个 **MapReduce 程序 `BuyerStat`**，统计「每位买家收藏的商品个数」。数据是买家收藏日志（买家id、商品id、收藏日期，制表符分隔），先准备 8 条原始数据，再用 Shell 脚本追加 120 条以自己学号为买家id 的数据，最后用 Maven 打包成 jar，通过 `hadoop jar` 提交到 Hadoop（YARN）集群运行，输出每个买家的收藏次数。

最终产出：HDFS 上 `/mr2out/part-r-00000` 文件，包含 6 个买家的统计结果，其中学号 `20242093` 对应 `120`。

---

## 2. 环境与前置条件

| 项 | 值 |
|---|---|
| Hadoop | 3.4.2（伪分布式） |
| 安装目录 | /usr/local/hadoop-3.4.2 |
| JDK | 11（编译和运行都用 JDK 11） |
| 开发工具 | Windows 上 IDEA + Maven |
| Maven 本地仓库 | D:\_local_repo |
| 虚拟机用户 | suzuha（原文 scp 命令中出现） |
| 虚拟机 IP | 192.168.88.129 |
| 实验数据目录 | /data/mapreduce2 |
| HDFS 输入目录 | /mapreduce2_in |
| HDFS 输出目录 | /mr2out |

**前置条件**：
- Hadoop 伪分布式环境已搭好（见实验1），`jps` 能看到 5 个进程
- 选用统计口径：**统计买家收藏的商品个数**（以买家id 为 key），这样自己的学号会出现在结果里，便于截图框注

---

## 3. 关键概念速览

1. **MapReduce**：Hadoop 的分布式计算框架，核心是「分而治之」——Map 阶段把输入拆成键值对处理，Reduce 阶段把相同 key 的值合并汇总，中间由框架自动完成 Shuffle（分组、排序、传输）。

2. **Mapper**：map 函数，输入一行数据，输出若干 `<key, value>` 中间键值对。本实验中输入一行收藏记录，输出 `<买家id, 1>`。

3. **Reducer**：reduce 函数，接收 `<key, values列表>`，把 values 累加。本实验中把同一买家的所有 `1` 加起来，得到收藏次数。

4. **Shuffle**：框架自动完成的阶段，把 Map 输出按 key 分组、排序，送到对应 Reducer。编程者不用管。

5. **Driver**：程序入口，配置 Job（设 Mapper/Reducer 类、输入输出路径、输出 key/value 类型等），提交作业。

6. **YARN**：Hadoop 的资源调度系统，MapReduce 作业要提交到 YARN 上运行才是真正的「集群模式」。由 `mapred-site.xml` 中 `mapreduce.framework.name` 决定用 local 还是 yarn。

7. **Maven**：Java 项目管理工具，在 `pom.xml` 声明依赖（如 `hadoop-client`），自动下载 jar 包，配合 `maven-assembly-plugin` 打成含全部依赖的 fat jar。

8. **fat jar（jar-with-dependencies）**：把所有依赖都打进一个 jar 包里，直接 `hadoop jar` 就能跑，不用手动配 classpath。

---

## 4. 完整操作步骤

### Step 1：启动 Hadoop 集群并验证

```bash
cd /usr/local/hadoop-3.4.2/sbin
./start-all.sh
jps
```

**预期**：启动日志依次出现 `Starting namenodes`、`Starting datanodes`、`Starting secondary namenodes`、`Starting resourcemanager`、`Starting nodemanagers`；`jps` 看到 5 个进程：NameNode、DataNode、SecondaryNameNode、ResourceManager、NodeManager。

> 日志中的 WARNING（如 "This is not a recommended production deployment configuration"）是伪分布式的正常提示，不影响。

### Step 2：准备原始数据（8 条）

数据格式：`买家id<Tab>商品id<Tab>收藏日期时间`，**不含表头行**。

```bash
mkdir -p /data/mapreduce2
cd /data/mapreduce2

printf '10181\t1000481\t2010-04-04 16:54:31\n' > buyer_favorite1
printf '20001\t1001597\t2010-04-07 15:07:52\n' >> buyer_favorite1
printf '20001\t1001560\t2010-04-07 15:08:27\n' >> buyer_favorite1
printf '20042\t1001368\t2010-04-08 08:20:30\n' >> buyer_favorite1
printf '20067\t1002061\t2010-04-08 16:45:33\n' >> buyer_favorite1
printf '20056\t1003289\t2010-04-12 10:50:55\n' >> buyer_favorite1
printf '20056\t1003290\t2010-04-12 11:57:35\n' >> buyer_favorite1
printf '20056\t1003292\t2010-04-12 12:05:29\n' >> buyer_favorite1
```

**验证**：
```bash
cat -A buyer_favorite1     # ^I 表示 Tab，$ 表示行尾，确认分隔符是真制表符
wc -l buyer_favorite1       # 应为 8
```

> 要点：第 1 行用 `>` 覆盖创建文件，其余用 `>>` 追加；`\t` 是制表符，`\n` 是换行符。

> ⚠️ 关于表头：作业样例首行"买家id 商品id 收藏日期"是列说明，**不是数据**。若误加到数据文件，Mapper 会把"买家id"当 key 输出，结果出现脏数据 `买家id\t1`。若要保留表头，需在 Mapper 里用正则过滤：`if (!fields[0].trim().matches("\\d+")) return;`。不要用 `if (key.get() == 0)` 判断首行——大文件分片后只有第一个分片的首行偏移量为 0。

### Step 3：用 Shell 脚本追加 120 条学号数据

创建脚本 `add_data.sh`：

```bash
#!/bin/bash
# 功能：向收藏数据文件末尾追加 120 行学号数据
FILE=/data/mapreduce2/buyer_favorite1
SID=20242093

for ((i=1; i<=120; i++))
do
    gid=$((1000000 + i * 7))
    day=$(( (i % 28) + 1 ))
    hour=$((10 + i % 8))
    min=$((i % 60))
    sec=$(( (i * 13) % 60 ))
    echo -e "${SID}\t${gid}\t2010-04-$(printf '%02d' $day) ${hour}:${min}:${sec}" >> $FILE
done

echo "done. total lines: $(wc -l < $FILE)"
```

执行：
```bash
chmod +x add_data.sh
./add_data.sh
```

**验证**：
```bash
wc -l buyer_favorite1              # 应为 128（8 + 120）
grep -c "^20242093" buyer_favorite1  # 应为 120
tail -5 buyer_favorite1
```

### Step 4：上传数据到 HDFS

MapReduce 只能读 HDFS 上的数据，不能直接读本地文件。

```bash
hadoop fs -mkdir -p /mapreduce2_in
hadoop fs -put buyer_favorite1 /mapreduce2_in/
hadoop fs -ls /mapreduce2_in
```

### Step 5：在 Windows 上用 IDEA + Maven 写代码并打包

#### 5.1 编写 BuyerStat.java

完整代码（含 Mapper、Reducer、Driver）：

```java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class BuyerStat {

    // Mapper：输入一行，输出 (买家ID, 1)
    public static class BuyerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text buyerId = new Text();

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t");       // 按制表符切分
            if (fields.length >= 1) {
                buyerId.set(fields[0]);               // 取第 1 个字段：买家ID
                context.write(buyerId, one);          // 输出（买家ID, 1）
            }
        }
    }

    // Reducer：把同一买家的所有 1 累加
    public static class BuyerReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();                     // 累加同一个 key 的所有 1
            }
            result.set(sum);
            context.write(key, result);               // 输出（买家ID, 总次数）
        }
    }

    // Driver：配置并提交作业
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "buyer-favorite-count");

        job.setJarByClass(BuyerStat.class);
        job.setMapperClass(BuyerMapper.class);
        job.setReducerClass(BuyerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));    // 输入路径

        // ====== 改进：输出目录若已存在，先用 HDFS Java API 自动删除 ======
        Path outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);        // true 表示递归删除
        }
        fs.close();
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```

#### 5.2 配置 pom.xml

关键依赖和插件：

```xml
<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-client</artifactId>
    <version>3.4.2</version>
</dependency>

<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-assembly-plugin</artifactId>
    <version>3.6.0</version>
    <configuration>
        <descriptorRefs>
            <descriptorRef>jar-with-dependencies</descriptorRef>
        </descriptorRefs>
        <archive>
            <manifest>
                <mainClass>com.examples.wordcount.BuyerStat</mainClass>
            </manifest>
        </archive>
    </configuration>
</plugin>
```

> 说明：`maven.compiler.source/target` 设为 11，和虚拟机 JDK 版本一致。

#### 5.3 打包

IDEA 右侧 Maven 面板 → Lifecycle → 双击 `package`，或命令行：

```bash
mvn clean package
```

**预期**：控制台末尾 `BUILD SUCCESS`，`target` 目录生成 `wordcount-1.0-SNAPSHOT-jar-with-dependencies.jar`（约 64MB，含 Hadoop 全部依赖）。

> 注意：`target` 下不带 `-jar-with-dependencies` 的 jar 缺少依赖，**不能**用于提交。Maven 输出的 WARNING 是插件版本校验警告，只要 `BUILD SUCCESS` 就成功。

### Step 6：传输 jar 到虚拟机

Windows CMD 中执行 scp：

```bash
scp D:\...\wordcount-1.0-SNAPSHOT-jar-with-dependencies.jar suzuha@192.168.88.129:/home/suzuha/
```

> 命令结构：`scp 源文件路径 用户名@虚拟机IP:目标路径`。首次连接输入 `yes` 确认指纹，再输密码（输入时屏幕不显示，正常）。也可用 Xftp 拖拽。

为便于输入，重命名为短名：

```bash
mv wordcount-1.0-SNAPSHOT-jar-with-dependencies.jar app.jar
```

验证 jar 内容：
```bash
jar tf app.jar | grep BuyerStat
```
**预期**：输出 `BuyerStat.class`、`BuyerStat$BuyerReducer.class`（`$` 表示内部类）。

### Step 7：提交作业到 Hadoop 运行

```bash
hadoop jar app.jar /mapreduce2_in /mr2out
```

> 命令结构：`hadoop jar <jar包> <输入路径> <输出路径>`。因 pom.xml 配了 mainClass，可省略类名。

**预期**：终端输出 map、reduce 进度（`map 0% reduce 0%` → `map 100% reduce 100%`），最后 `completed successfully`。

> ⚠️ 输出目录 `/mr2out` 必须事先不存在，否则报 `Output directory already exists`。本实验代码已加自动删除逻辑（见 Step 5.1），无需手动删；若用未改进版本，需先 `hadoop fs -rm -r /mr2out`。

### Step 8：查看结果

```bash
hdfs dfs -ls /mr2out
hadoop fs -cat /mr2out/part-r-00000
```

**预期输出**（6 个买家）：
```
10181	1
20001	2
20042	1
20056	3
20067	1
20242093	120
```

下载到本地核对：
```bash
hadoop fs -get /mr2out/part-r-00000 /data/mapreduce2/result.txt
cat /data/mapreduce2/result.txt
```

### Step 9（重要）：配置 MapReduce 运行在 YARN 集群上

**问题背景**：前面运行虽成功，但日志显示作业 ID 是 `job_local...`、运行器是 `LocalJobRunner`、跟踪页是 `http://localhost:8080/`，说明作业跑在**本地模式**，没提交到 YARN。

**原因**：`mapred-site.xml` 中 `mapreduce.framework.name` 默认是 `local`。即使 YARN 进程已启动，作业仍由 LocalJobRunner 本地执行。

#### 9.1 修改 mapred-site.xml

路径：`/usr/local/hadoop-3.4.2/etc/hadoop/mapred-site.xml`

```xml
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    <property>
        <name>yarn.app.mapreduce.am.env</name>
        <value>HADOOP_MAPRED_HOME=/usr/local/hadoop-3.4.2</value>
    </property>
    <property>
        <name>mapreduce.map.env</name>
        <value>HADOOP_MAPRED_HOME=/usr/local/hadoop-3.4.2</value>
    </property>
    <property>
        <name>mapreduce.reduce.env</name>
        <value>HADOOP_MAPRED_HOME=/usr/local/hadoop-3.4.2</value>
    </property>
</configuration>
```

> 后三项为 ApplicationMaster、Map、Reduce 任务指明 MapReduce 框架目录，避免"找不到 MapReduce 框架"错误。

#### 9.2 修改 yarn-site.xml

路径：`/usr/local/hadoop-3.4.2/etc/hadoop/yarn-site.xml`

```xml
<configuration>
    <property>
        <name>yarn.resourcemanager.hostname</name>
        <value>localhost</value>
    </property>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
</configuration>
```

> `yarn.nodemanager.aux-services=mapreduce_shuffle` 是 NodeManager 上的辅助服务，负责 Reduce 阶段从各 Map 节点拉数据。缺失则 Reduce 卡在 Shuffle。

> ⚠️ 坑：`mapreduce_shuffle` 的 value 前**不能有空格**！原文就因为写成 `" mapreduce_shuffle"`（多了个空格），导致 NodeManager 启动时找不到 ShuffleHandler 实现类而退出，`jps` 看不到 NodeManager。

#### 9.3 重启 YARN 并验证

```bash
stop-yarn.sh
start-yarn.sh
jps
```

**预期**：5 个进程齐全。直接提交作业（不删输出目录，测试自动删除）：

```bash
hadoop jar /home/suzuha/app.jar /mapreduce2_in /mr2out
```

**验证作业跑在 YARN 上**：日志中应出现：
- 作业 ID：`job_1791879286351_0002`（不再是 `job_local...`）
- 应用 ID：`application_1791879286351_0002`
- 跟踪页：`http://192.168.88.129:8088/proxy/application_.../`
- 连接信息：`Connecting to ResourceManager at localhost/127.0.0.1:8032`
- 计数器：`Launched map tasks=1`、`Launched reduce tasks=1`、`Data-local map tasks=1`

---

## 5. 代码讲解

### 5.1 Mapper

```java
protected void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
    String line = value.toString();
    String[] fields = line.split("\t");       // 按制表符切分三列
    if (fields.length >= 1) {
        buyerId.set(fields[0]);               // 取第 1 列：买家ID
        context.write(buyerId, one);          // 输出 (买家ID, 1)
    }
}
```

- 输入：一行文本 `买家id\t商品id\t日期时间`
- 处理：`split("\t")` 按制表符切成数组，`fields[0]` 是买家id
- 输出：`<买家id, 1>`，每个收藏记 1 次

> 若要改成统计「商品被收藏次数」，只需把 `fields[0]` 改成 `fields[1]`。这体现了 MapReduce「key 决定统计维度」。

### 5.2 Reducer

```java
protected void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
    int sum = 0;
    for (IntWritable val : values) {
        sum += val.get();                     // 累加同一买家的所有 1
    }
    result.set(sum);
    context.write(key, result);               // 输出 (买家ID, 总次数)
}
```

- 输入：`<买家id, [1, 1, 1, ...]>`（Shuffle 已把同 key 的值聚合成列表）
- 处理：遍历 values 累加
- 输出：`<买家id, 总次数>`

### 5.3 Driver 中的输出目录自动删除（改进点）

```java
Path outputPath = new Path(args[1]);
FileSystem fs = FileSystem.get(conf);
if (fs.exists(outputPath)) {
    fs.delete(outputPath, true);        // true = 递归删除目录
}
fs.close();
FileOutputFormat.setOutputPath(job, outputPath);
```

- `FileSystem.get(conf)`：根据配置获取 HDFS 文件系统对象
- `exists(Path)`：判断路径是否存在
- `delete(Path, boolean)`：删除文件/目录，`true` 递归删
- 作用：重跑作业时无需手动 `hadoop fs -rm -r`，程序自动清理

---

## 6. 截图对照说明

> 原文截图共 32 张，以下挑与操作步骤相关的关键截图转写。

- **第 1–2 张图**：实验封面、信息表。
- **第 3–5 张图**：`start-all.sh` 启动日志，依次显示 Starting namenodes/datanodes/secondary namenodes/resourcemanager/nodemanagers；`jps` 显示 5 个进程。
- **第 6–8 张图**：`printf` 写入 8 条原始数据；`cat -A` 显示 `^I`（Tab）和 `$`（行尾）确认制表符；`wc -l` 显示 8。
- **第 9–11 张图**：`add_data.sh` 脚本内容；`./add_data.sh` 执行输出 `done. total lines: 128`；`grep -c "^20242093"` 输出 120。
- **第 12–13 张图**：`hadoop fs -mkdir -p /mapreduce2_in`、`-put` 上传、`-ls` 确认。
- **第 14–16 张图**：IDEA 中 BuyerStat 代码；pom.xml 依赖和插件配置；`mvn package` 输出 `BUILD SUCCESS`，target 目录有 jar-with-dependencies.jar。
- **第 17–19 张图**：scp 传输 jar 到虚拟机；`mv` 重命名为 app.jar；`jar tf app.jar | grep BuyerStat` 确认类存在。
- **第 20–22 张图**：`hadoop jar app.jar /mapreduce2_in /mr2out` 运行，map/reduce 进度到 100%；`hadoop fs -cat /mr2out/part-r-00000` 输出 6 行结果，`20242093 120` 被红框标注。
- **第 23–26 张图**：发现作业 ID 是 `job_local...` 判定为本地模式；修改 `mapred-site.xml`（`mapreduce.framework.name=yarn`）和 `yarn-site.xml`；错误截图中 `yarn.nodemanager.aux-services` 的 value 前多了空格导致 NodeManager 缺失。
- **第 27–29 张图**：修正空格后 `stop-yarn.sh` / `start-yarn.sh` 重启，`jps` 5 进程齐全；重新提交作业，日志显示 `job_1791879286351_0002`、`Connecting to ResourceManager at localhost/127.0.0.1:8032`、`completed successfully`。
- **第 30–32 张图**：验证输出目录已存在时直接提交仍成功（自动删除生效）；`hadoop fs -cat` 结果一致；本地 `grep -c` 与 MapReduce 结果对比验证。

---

## 7. 常见报错与排查

### 原文提到过的问题

| 问题 | 原因 | 解决 |
|---|---|---|
| `Output directory already exists` | 输出目录 `/mr2out` 已存在 | 重跑前 `hadoop fs -rm -r /mr2out`，或在代码中用 HDFS Java API 自动删除（本实验改进版已实现） |
| 作业 ID 是 `job_local...`，运行器 `LocalJobRunner` | `mapreduce.framework.name` 默认为 `local` | 在 `mapred-site.xml` 设为 `yarn`，并配置 `yarn.app.mapreduce.am.env` 等三项 |
| `jps` 缺少 NodeManager | `yarn-site.xml` 中 `yarn.nodemanager.aux-services` 的 value 前多了空格 | 删掉空格，确保 `<value>mapreduce_shuffle</value>`，重启 YARN |
| `cat -A` 看不到 `^I` | 分隔符不是真制表符（可能是空格） | 用 `printf '\t'` 而非空格，或检查数据文件 |
| 结果中出现 `买家id 1` 脏数据 | 数据文件误加了表头行 | 数据文件不含表头；或在 Mapper 中用正则过滤非数字买家id |

### 按环境差异推断的问题

| 问题 | 可能原因 | 排查方向 |
|---|---|---|
| `hadoop jar` 报 `ClassNotFoundException` | 用了不带 `-jar-with-dependencies` 的 jar，或 mainClass 配错 | 用 `jar-with-dependencies` 的 jar；检查 pom.xml 的 `<mainClass>` 全限定类名 |
| Reduce 阶段卡住 | `yarn.nodemanager.aux-services` 没配 `mapreduce_shuffle` | 检查 yarn-site.xml，重启 YARN |
| `scp` 连不上虚拟机 | 防火墙没关或 IP 变了 | `ip addr` 确认 IP；`sudo ufw disable` 关防火墙 |
| Windows 打的 jar 在 Linux 跑不了 | JDK 版本不一致（编译用高版本，运行用低版本） | `maven.compiler.source/target` 设为和虚拟机一致的 11 |
| `mvn package` 下载依赖失败 | 网络或 Maven 仓库配置问题 | 检查 `settings.xml` 镜像配置；或手动下载 hadoop-client 3.4.2 放入本地仓库 |

---

## 8. 自检清单

- [ ] `jps` 看到 5 个进程（含 NodeManager）
- [ ] `wc -l buyer_favorite1` = 128，`grep -c "^20242093"` = 120
- [ ] `hadoop fs -ls /mapreduce2_in` 能看到 buyer_favorite1
- [ ] `mvn package` 输出 `BUILD SUCCESS`，target 有 `jar-with-dependencies.jar`
- [ ] `jar tf app.jar | grep BuyerStat` 能看到 BuyerStat.class
- [ ] `hadoop jar app.jar /mapreduce2_in /mr2out` 完成，进度到 100%
- [ ] `hadoop fs -cat /mr2out/part-r-00000` 输出 6 行，`20242093 120`
- [ ] 作业 ID 是 `job_数字_序号`（非 `job_local...`），确认跑在 YARN 上
- [ ] 输出目录已存在时直接提交仍成功（自动删除改进生效）

---

## 9. 小结 + 易忘命令速记

**小结**：本实验用 Java + Maven 写了一个统计买家收藏次数的 MapReduce 程序，完整链路是「准备数据（8条原始 + Shell 脚本追加 120 条学号数据）→ 写 Mapper/Reducer/Driver → Maven 打 fat jar → scp 传虚拟机 → `hadoop jar` 提交 → 查看结果」。两个关键改进：① 配置 `mapreduce.framework.name=yarn` 让作业真正跑在 YARN 集群而非本地模式；② 用 HDFS Java API 在 Driver 中自动删除已存在的输出目录。核心收获是理解 MapReduce 的 Map→Shuffle→Reduce 流程，以及「key 决定统计维度」。

**易忘命令速记**：

```bash
# 启动/重启
cd /usr/local/hadoop-3.4.2/sbin
./start-all.sh
stop-yarn.sh && start-yarn.sh    # 改完 yarn 配置后只重启 YARN
jps

# 数据
printf 'a\tb\tc\n' > file          # 写带制表符的行
cat -A file                        # 看 Tab(^I) 和行尾($)
wc -l file                         # 行数
grep -c "^20242093" file           # 统计某开头的行数

# HDFS
hadoop fs -mkdir -p /dir
hadoop fs -put 本地文件 /hdfs路径
hadoop fs -cat /hdfs/part-r-00000
hadoop fs -rm -r /hdfs路径         # 递归删
hadoop fs -get /hdfs/文件 本地路径  # 下载

# 提交作业
hadoop jar app.jar 输入路径 输出路径

# 打包
mvn clean package

# 传文件
scp 本地文件 用户名@IP:远程路径
```
