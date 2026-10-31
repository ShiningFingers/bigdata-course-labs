# 实验6：Hive 数据仓库安装部署与 WordCount 实现学习笔记

> 源报告：`20242093刘浩宇6.docx`（实验序号：6，主题：Hive 数据仓库安装部署与 WordCount 实现）

---

## 1. 这个实验在做什么

在已有 Hadoop 集群上**安装部署 Hive 4.0.1**（用 MySQL 存元数据），然后用 **HQL** 建库、建表、导入数据，并跑一条 **WordCount 词频统计**查询，理解 HQL 如何被翻译成 MapReduce 作业执行。最终产出一张词频表。

---

## 2. 环境与前置条件

| 项 | 内容 |
|---|---|
| 操作系统 | Linux（用户 `suzuha`，主机名 `lhy`） |
| JDK | 11.0.2（路径 `/usr/local/jdk-11.0.2`） |
| Hadoop | 3.4.2（路径 `/usr/local/hadoop-3.4.2`），已能启动 |
| MySQL | 8.0（已安装，root 密码 `111111`） |
| Hive | 4.0.1（路径 `/usr/local/hive-4.0.1`）—— 选这个版本是因为 Hive 4.1 要 Java 17、4.2 要 JDK 21，而本机是 JDK 11；Hive 3.1.3 在 JDK 11 下会抛 `ClassCastException` |
| MySQL 驱动 | `com.mysql.cj.jdbc.Driver`（需把 mysql-connector jar 放到 Hive 的 lib 目录） |

**前置准备顺序**：
```
JDK 11 → Hadoop 3.4.2 启动 → MySQL 8.0 安装
→ Hive 4.0.1 解压 → 环境变量 → hive-site.xml → MySQL 驱动 jar
→ HDFS 建目录赋权 → schematool 初始化元数据
```

---

## 3. 关键概念速览

- **Hive**：建立在 Hadoop 上的数据仓库工具，把 HQL（类似 SQL）翻译成 MapReduce 作业执行。它不存数据，只管理元数据。
- **元数据（Metastore）**：描述"库表结构、字段、文件位置"等信息的数据，本实验存在 MySQL 的 `hive` 库里。数据本身存在 HDFS 上。
- **`hive.metastore.warehouse.dir`**：Hive 数据仓库在 HDFS 上的根目录，默认是 `/user/hive/warehouse`。
- **库表与 HDFS 目录的映射**：Hive 的"库"对应 HDFS 上的 `库名.db` 目录，"表"对应库目录下的同名子目录。建库建表本质是在 HDFS 上建目录。
- **LOAD DATA**：把文件"搬运"到表对应的 HDFS 目录，不做格式转换。
- **执行引擎**：Hive 可以用 MapReduce、Tez、Spark 等引擎执行。本实验用 `SET hive.execution.engine=mr;` 切到 MapReduce。
- **Beeline**：Hive 4.0 的命令行客户端（旧版 CLI 已废弃），通过 JDBC 连接 Hive。
- **LATERAL VIEW explode**：HQL 语法，把数组炸开成多行，用于把一行文本按空格拆成多个单词。

---

## 4. 完整操作步骤

### Step 1：启动基础服务

```bash
start-dfs.sh
start-yarn.sh
systemctl status mysql
```

**说明**：启动 HDFS、YARN，并确认 MySQL 在运行。

**正常现象**：`jps` 有 NameNode/DataNode/ResourceManager/NodeManager；MySQL 显示 `active (running)`。

### Step 2：配置 `hive-site.xml`（`$HIVE_HOME/conf/`）

```xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>/user/hive/warehouse</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:mysql://localhost:3306/hive?createDatabaseIfNotExist=true</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>com.mysql.cj.jdbc.Driver</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value>root</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionPassword</name>
    <value>111111</value>
  </property>
</configuration>
```

**说明**：
- `hive.metastore.warehouse.dir`：数据仓库在 HDFS 的位置。
- `ConnectionURL`：连 MySQL 的 `hive` 库，不存在则自动创建。
- `ConnectionDriverName` / `UserName` / `Password`：MySQL 驱动类、用户名、密码。

> 原文截图中 hive-site.xml 末尾被截断，`ConnectionPassword` 的 `111111` 是根据 schematool 命令 `root 111111` 推断的，原文未在截图中完整展示 password 那一行的 value。

### Step 3：配置 `hive-env.sh`

```bash
export JAVA_HOME=/usr/local/jdk-11.0.2
export HADOOP_HOME=/usr/local/hadoop-3.4.2
```

**说明**：告诉 Hive 去哪里找 JDK 和 Hadoop。

### Step 4：HDFS 建目录并赋权

```bash
hdfs dfs -mkdir -p /tmp
hdfs dfs -mkdir -p /user/hive/warehouse
hdfs dfs -chmod g+w /tmp
hdfs dfs -chmod g+w /user/hive/warehouse
```

**说明**：Hive 需要在 HDFS 上写临时文件和仓库数据，所以给这两个目录加组写权限。

### Step 5：初始化元数据

```bash
schematool -dbType mysql -initSchema root 111111
```

**说明**：在 MySQL 的 `hive` 库里创建 Hive 所需的元数据表（70 余张）。`root 111111` 是 MySQL 的用户名和密码。

**正常现象**：输出 `Initialization script hive-schema-4.0.0.mysql.sql`，无报错。

### Step 6：启动 Hive 客户端并建立连接

```bash
hive
```

进入后提示符是 `beeline>`（Hive 4.0 的 hive 命令实际是 Beeline），需要先连 JDBC：

```
beeline> !connect jdbc:hive2://
```

用户名和密码都直接回车（嵌入式模式，无需凭据），连接成功后提示符变为 `0: jdbc:hive2://>`。

**说明**：Hive 4.0 废弃了旧版 CLI，`hive` 命令是 Beeline 的封装，必须先 `!connect` 建立连接才能执行 HQL。

### Step 7：建库建表

```sql
CREATE DATABASE db_20242093;
USE db_20242093;
CREATE TABLE wc_20242093 (line string) STORED AS TEXTFILE;
SHOW TABLES;
DESCRIBE wc_20242093;
```

**说明**：
- `CREATE DATABASE db_20242093;`：建库（库名带学号）。
- `USE db_20242093;`：切换到该库。
- `CREATE TABLE wc_20242093 (line string) STORED AS TEXTFILE;`：建表，只有一个 `line` 字段（字符串），存为文本文件。
- `SHOW TABLES;` / `DESCRIBE wc_20242093;`：确认表存在和结构。

**正常现象**：`SHOW TABLES` 返回 `wc_20242093`；`DESCRIBE` 显示 `line string`。

### Step 8：查看 HDFS 目录结构

```bash
hdfs dfs -ls -R /user/hive/warehouse
```

**说明**：验证"库表即目录"。建库后 HDFS 出现 `/user/hive/warehouse/db_20242093.db`，建表后该目录下出现 `wc_20242093` 子目录。

**正常现象**：
```
drwxr-xr-x  /user/hive/warehouse/db_20242093.db
drwxr-xr-x  /user/hive/warehouse/db_20242093.db/wc_20242093
```

### Step 9：准备数据文件

```bash
mkdir -p ~/data
cat > ~/data/1.txt << 'EOF'
20242093 刘浩宇
hello world
hello hive
20242093 刘浩宇
hive wordcount
hadoop hive
20242093 hadoop
EOF
```

**说明**：在本地创建 7 行测试数据，每行用空格分隔单词。学号 `20242093` 出现 3 次，姓名 `刘浩宇` 出现 2 次。

### Step 10：导入数据

```sql
LOAD DATA LOCAL INPATH '/home/suzuha/data/1.txt' INTO TABLE wc_20242093;
SELECT * FROM wc_20242093;
```

**说明**：
- `LOAD DATA LOCAL INPATH`：把本地文件搬到 Hive 表对应的 HDFS 目录（`/user/hive/warehouse/db_20242093.db/wc_20242093/`），文件名还是 `1.txt`。
- `SELECT *`：查询表内容，确认数据导入成功。

**正常现象**：返回 7 行，每行是原文件的一行。

### Step 11：切换执行引擎并提交 WordCount

```sql
SET hive.execution.engine=mr;

SELECT word, count(1) AS cnt
FROM wc_20242093
LATERAL VIEW explode(split(line, ' ')) t AS word
GROUP BY word
ORDER BY cnt DESC;
```

**说明**：
- `SET hive.execution.engine=mr;`：把执行引擎切到 MapReduce（Hive 4.0 默认是 Tez，但环境没装 Tez）。
- `split(line, ' ')`：把每行按空格切成数组。
- `explode(...)`：把数组炸开成多行，每行一个单词。
- `LATERAL VIEW ... t AS word`：给炸开的列取名 `word`。
- `GROUP BY word` + `count(1)`：按单词分组计数。
- `ORDER BY cnt DESC`：按出现次数降序排列。

**正常现象**：控制台显示两个 MapReduce 作业（Stage-1 分组计数，Stage-2 全局排序），最后输出 7 行词频结果：
```
word        cnt
hive        3
20242093    3
刘浩宇       2
hello       2
hadoop      2
world       1
wordcount   1
```
耗时约 44 秒。

---

## 5. 代码讲解

本实验的"代码"主要是 HQL。核心就是 Step 11 那条 WordCount 查询：

```sql
SELECT word, count(1) AS cnt
FROM wc_20242093
LATERAL VIEW explode(split(line, ' ')) t AS word
GROUP BY word
ORDER BY cnt DESC;
```

**逐段解释**：

| 子句 | 作用 |
|---|---|
| `split(line, ' ')` | 把 `line` 字段（一行文本）按空格切分成字符串数组，如 `"hello world"` → `["hello", "world"]` |
| `explode(数组)` | 把数组"炸开"成多行，每个元素一行 |
| `LATERAL VIEW ... t AS word` | 把炸开的结果作为虚拟表 `t`，列名叫 `word`，与原表关联 |
| `GROUP BY word` | 按单词分组 |
| `count(1) AS cnt` | 统计每组的行数，即该单词出现次数，别名 `cnt` |
| `ORDER BY cnt DESC` | 按次数降序排序 |

**为什么会生成两个 MapReduce 作业？**
- **Stage-1**：`GROUP BY` 分组计数。Map 端按词分发，Reduce 端累加。
- **Stage-2**：`ORDER BY` 全局排序。全局有序要求所有结果由**一个** Reducer 汇总，无法和前一阶段合并，所以独立成第二个作业。日志里 `Number of reduce tasks determined at compile time: 1` 就是这个意思。

---

## 6. 截图对照说明

| 图号 | 内容 | 文字转写 |
|---|---|---|
| 图1 | MySQL 服务运行状态 | `mysql.service - MySQL Community Server`，`Active: active (running)`，PID 6307，内存 354.7M |
| 图2 | hive-site.xml 核心配置 | 见 Step 2 的 XML，含 warehouse.dir、ConnectionURL、ConnectionDriverName、ConnectionUserName、ConnectionPassword |
| 图3 | schematool 元数据初始化成功 | `Initializing the schema to: 4.0.0`，`Metastore connection URL: jdbc:mysql://localhost:3306/hive?...`，`Starting metastore schema initialization to 4.0.0` |
| 图4 | 建库与建表命令执行结果 | `CREATE DATABASE db_20242093` → `No rows affected`；`CREATE TABLE` → `No rows affected`；`SHOW TABLES` → `wc_20242093`；`DESCRIBE` → `line string` |
| 图5 | HDFS 中库表对应的目录结构 | `/user/hive/warehouse/db_20242093.db/wc_20242093/1.txt`（104 字节），证实库→目录、表→子目录、数据文件原样存放 |
| 图6 | 测试数据集内容 | `cat ~/data/1.txt` 输出 7 行，含 `20242093 刘浩宇` 等 |
| 图7 | 数据导入后的查询结果 | `SELECT *` 返回 7 行；修复编码前中文显示为 `???`，修复后正常显示 `刘浩宇` |
| 图8 | WordCount 词频统计最终结果 | 见 Step 11 的 7 行结果，`7 rows selected (44.283 seconds)` |

---

## 7. 常见报错与排查

### 原文提到过的问题

**问题一：`hive` 命令进入的是 Beeline，直接执行 HQL 无响应**

- **现象**：输入 `hive` 后提示符是 `beeline>`，执行 `SHOW DATABASES;` 没反应。
- **原因**：Hive 4.0 废弃旧版 CLI，`hive` 是 Beeline 的封装，必须先建立 JDBC 连接。
- **解决**：在 `beeline>` 下执行 `!connect jdbc:hive2://`，用户名密码两次回车。

**问题二：查询结果中文显示为问号（`20242093 ???`）**

- **现象**：`SELECT *` 中"刘浩宇"显示为三个问号。
- **排查**：`hdfs dfs -cat /user/hive/warehouse/.../1.txt` 显示中文正常 → 数据没坏，是 Beeline 输出编码问题。
- **解决**：启动 Hive 前设置 JVM 编码：
  ```bash
  export HADOOP_CLIENT_OPTS="-Dfile.encoding=UTF-8"
  # 或
  export JAVA_TOOL_OPTIONS="-Dfile.encoding=UTF-8"
  hive
  ```
- **注意**：不能写入 `HIVE_OPTS`，该变量会被拼接到 Java 类名之后，导致 `Unrecognized option` 报错。
- **永久化**：把 `export` 追加到 `~/.bashrc`。

**问题三：MapReduce 作业失败，`NoClassDefFoundError: org/apache/commons/collections/CollectionUtils`**

- **现象**：作业到 `map 100%, reduce 100%` 后失败，返回码 2。
- **原因**：Hadoop 3.x 因安全原因移除了 `commons-collections` 3.x，而 Hive 4.0.1 仍依赖它。注意是 `commons.collections`（3.x），不是 `commons.collections4`。而且任务在 YARN Container 的 JVM 里跑，classpath 来自 Hadoop，所以只放 Hive 的 lib 目录不够。
- **解决**：下载 jar 并同时放到 Hive 和 Hadoop 的 lib 目录：
  ```bash
  cd /usr/local/hive-4.0.1/lib
  wget https://repo1.maven.org/maven2/commons-collections/commons-collections/3.2.2/commons-collections-3.2.2.jar
  cp commons-collections-3.2.2.jar /usr/local/hadoop-3.4.2/share/hadoop/common/lib/
  ```
  然后重启 Hive 客户端（Hive 启动时扫描 lib 目录），无需重启 Hadoop。

**问题四：Hive 4.0 默认执行引擎为 Tez，环境没装导致失败**

- **解决**：执行 `SET hive.execution.engine=mr;` 切换为 MapReduce。日志里 `Hive-on-MR is deprecated` 是警告，不影响执行。

### 按环境差异推断的排查方向

- **MySQL 连接失败**：检查 MySQL 是否启动、密码是否正确、`hive-site.xml` 里的 URL/用户名/密码是否一致。
- **`schematool` 初始化失败**：检查 MySQL 驱动 jar 是否在 `$HIVE_HOME/lib` 下，驱动类名是否是 `com.mysql.cj.jdbc.Driver`（MySQL 8.0 用 `cj`，不是旧的 `com.mysql.jdbc.Driver`）。
- **HDFS 权限报错**：确认 `/tmp` 和 `/user/hive/warehouse` 有组写权限（Step 4）。
- **执行引擎报错**：如果没装 Tez/Spark，记得 `SET hive.execution.engine=mr;`。

---

## 8. 自检清单

- [ ] `jps` 有 NameNode、DataNode、ResourceManager、NodeManager
- [ ] `systemctl status mysql` 显示 `active (running)`
- [ ] `schematool -dbType mysql -initSchema root 111111` 无报错
- [ ] `!connect jdbc:hive2://` 后提示符变为 `0: jdbc:hive2://>`
- [ ] `SHOW TABLES;` 返回 `wc_20242093`
- [ ] `hdfs dfs -ls -R /user/hive/warehouse` 能看到 `db_20242093.db/wc_20242093/1.txt`
- [ ] `SELECT * FROM wc_20242093;` 返回 7 行，中文正常显示
- [ ] WordCount 查询返回 7 行，`hive` 和 `20242093` 各 3 次，`刘浩宇`、`hello`、`hadoop` 各 2 次，`world`、`wordcount` 各 1 次

---

## 9. 小结与易忘命令速记

**一句话小结**：Hive 是"HQL→MapReduce"的翻译层 + 元数据管理者，数据存 HDFS、元数据存 MySQL；本实验用 Hive 4.0.1 + MySQL 8.0 搭好环境后，用一条带 `LATERAL VIEW explode` 的 HQL 实现 WordCount，核心坑是 Beeline 连接、UTF-8 编码、commons-collections 依赖、执行引擎切 MR。

**易忘命令速记**：
```bash
start-dfs.sh && start-yarn.sh                    # 启动 Hadoop
schematool -dbType mysql -initSchema root 111111  # 初始化元数据
hive                                              # 启动 Beeline
!connect jdbc:hive2://                            # 嵌入式连接（两次回车）
SET hive.execution.engine=mr;                     # 切 MapReduce 引擎
export HADOOP_CLIENT_OPTS="-Dfile.encoding=UTF-8" # 修中文乱码
hdfs dfs -ls -R /user/hive/warehouse              # 看库表目录结构
```
