# 实验7：Hive JDBC 操作 —— 学习笔记

## 1. 一句话说明这个实验在做什么

在 Hive 中建一张成绩表并导入 CSV 数据，然后用 **Java 通过 JDBC 连接 HiveServer2**，对表执行三个查询（本人各科成绩、平均分、最高分记录），最终产出一个可独立编译运行的 `HiveJDBCTest.java` 程序。

## 2. 环境与前置条件

| 项 | 值 | 说明 |
|---|---|---|
| Hive 版本 | 4.0.1 | 从 JDBC 驱动路径 `/usr/local/hive-4.0.1/jdbc/` 推断 |
| Hadoop 版本 | 3.4.2 | 来自 Hadoop Web UI 截图 |
| 操作系统用户 | suzuha | 所有路径以 `/home/suzuha/` 开头 |
| HiveServer2 端口 | 10000 | JDBC URL 中 `localhost:10000` |
| HDFS NameNode | localhost:9000 | Web UI 显示 `active` |
| Hadoop Web UI | http://localhost:9870 | 浏览 HDFS 文件 |
| Hive 仓库根目录 | /user/hive/warehouse | Hive 表在 HDFS 上的物理根路径 |
| JDK 版本 | （原文未说明） | 同机实验8为 JDK 11.0.2，可作参考 |

**前置启动顺序**（按依赖链）：
1. 启动 HDFS（`start-dfs.sh`）—— Hive 数据存在 HDFS 上
2. 启动 Hive 元数据库（MySQL）—— Hive 表结构存在 MySQL
3. 启动 HiveServer2（`hiveserver2`）—— Java JDBC 连接的服务端

## 3. 关键概念速览

- **JDBC**：Java Database Connectivity，Java 访问数据库的标准接口。只要换驱动类名和 URL，同一段代码就能连 MySQL、Hive、Oracle 等。
- **HiveServer2**：Hive 的服务端进程，监听 10000 端口，对外提供 JDBC/Thrift 连接。Java 程序不直接碰 Hive 数据文件，而是把 SQL 发给 HiveServer2 执行。
- **Beeline**：HiveServer2 的官方命令行客户端，底层走的也是 JDBC 协议。用它验证连通性，等于提前把 Java 程序的连接链路走一遍。
- **LOAD DATA LOCAL INPATH**：把 Linux 本地文件"搬运"到 HDFS 的表目录下。注意是搬文件，不是解析行数。
- **元数据 vs 数据**：Hive 的表结构（列名、类型、分隔符）存在 MySQL（元数据），实际数据行存在 HDFS（数据文件）。
- **本地模式**（`hive.exec.mode.local.auto=true`）：让 MapReduce 作业在本地进程内执行，不提交给 YARN。单机小数据量时能避免 YARN 调度导致的卡死。
- **Hive 不是数据库，是翻译器**：把 HQL 翻译成 MapReduce 任务去跑，延迟比 MySQL 高，但能处理的数据规模大得多。

## 4. 完整操作步骤

### Step 1：准备测试数据（本地 CSV 文件）

```bash
mkdir -p /home/suzuha/data
cat > /home/suzuha/data/score.txt << 'EOF'
1,20242093,刘浩宇,大数据系统基础,92
2,20242093,刘浩宇,数据库原理,85
3,20242093,刘浩宇,Java程序设计,88
4,20242093,刘浩宇,数据挖掘,95
5,20242093,刘浩宇,机器学习,78
6,20242093,刘浩宇,云计算,90
7,20242094,张三,大数据系统基础,76
8,20242095,李四,数据库原理,83
EOF
```

- 干什么：创建 8 行逗号分隔的成绩数据，前 6 行是本人（学号 20242093），后 2 行是其他同学，用于验证 `WHERE` 过滤是否生效。
- 为什么用逗号：逗号就是 CSV 标准分隔符，建表时 `FIELDS TERMINATED BY ','` 要与文件实际分隔符**完全一致**，否则整行会被塞进第一列，其他列全是 NULL。
- 预期：生成 `/home/suzuha/data/score.txt`，共 315 字节。

### Step 2：建库、建表

在 Hive 客户端（或 Beeline）中执行：

```sql
-- 建库（如已存在可跳过）
CREATE DATABASE db_20242093;
USE db_20242093;

-- 建表（5列，逗号分隔，文本文件存储）
CREATE TABLE score_20242093 (
  id     INT,
  sid    STRING,
  name   STRING,
  course STRING,
  score  INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
```

- 干什么：创建专属库 `db_20242093`（库名带学号，避免多人共用机器时表名冲突），在库中建 5 列成绩表。
- 为什么先建库再建表：Hive 的库在 HDFS 上就是一个 `库名.db` 文件夹，表是它的子文件夹。不建库直接建表，表会落到 `default` 库，容易与他人重名。
- 预期：无报错，表创建成功。

### Step 3：导入数据

```sql
LOAD DATA LOCAL INPATH '/home/suzuha/data/score.txt' INTO TABLE score_20242093;
```

- 干什么：把本地文件搬进 HDFS 的表目录。
- 关键日志（成功标志）：
  ```
  INFO : Loading data to table db_20242093.score_20242093 from file:/home/suzuha/data/score.txt
  INFO : Table db_20242093.score_20242093 stats: [numFiles=1, numRows=0, totalSize=315, ...]
  No rows affected (1.308 seconds)
  ```
- ⚠️ **注意 `numRows=0` 不是失败**：`LOAD DATA` 只搬文件不数行，所以 `numRows` 恒为 0。判断成功看 `numFiles=1`（有 1 个文件）和 `totalSize=315`（文件大小 315 字节，正好是 8 行数据的大小）。

### Step 4：用 Beeline 验证数据

```bash
beeline -u jdbc:hive2://localhost:10000/db_20242093
```

进入 Beeline 后执行：

```sql
SELECT * FROM score_20242093;
```

- 预期输出（对应报告截图）：8 行 5 列，中文显示正常，末尾 `8 rows selected (2.652 seconds)`，提示符变为 `0: jdbc:hive2://localhost:10000>`。
- 为什么用 Beeline 而不是 `hive` 命令：Beeline 走 JDBC 协议连 HiveServer2，和后面 Java 程序走的是同一条链路，先用它验证能提前暴露连接问题。

### Step 5：用 Hadoop Web UI 验证 HDFS 落盘

浏览器访问 `http://localhost:9870` → 菜单 **Utilities → Browse the file system**，进入路径：

```
/user/hive/warehouse/db_20242093.db/score_20242093
```

应看到文件 `score.txt`，权限 `-rw-r--r--`，属主 `suzuha`，属组 `supergroup`，大小 **315 B**，副本数 1，块大小 128 MB。

- 这条路径验证了 Hive 三层映射：仓库根 `/user/hive/warehouse` → 库目录 `db_20242093.db` → 表目录 `score_20242093` → 数据文件 `score.txt`。文件大小 315 B 与导入日志 `totalSize=315` 完全一致，形成闭环。

### Step 6：编写 Java JDBC 程序

文件路径：`/home/suzuha/hivejdbc/HiveJDBCTest.java`

完整代码见第 5 节。程序做三件事：
1. 加载 Hive JDBC 驱动、建立连接
2. 开启本地模式（`SET hive.exec.mode.local.auto=true`）
3. 执行三个查询并打印结果

### Step 7：编译与运行

```bash
# 编译
javac -encoding UTF-8 \
  -cp /usr/local/hive-4.0.1/jdbc/hive-jdbc-4.0.1-standalone.jar \
  -d /home/suzuha/hivejdbc /home/suzuha/hivejdbc/HiveJDBCTest.java

# 运行
java -Dfile.encoding=UTF-8 \
  -cp /home/suzuha/hivejdbc:/usr/local/hive-4.0.1/jdbc/hive-jdbc-4.0.1-standalone.jar \
  HiveJDBCTest
```

- `-encoding UTF-8`：让 javac 正确读取源码中的中文。
- `-Dfile.encoding=UTF-8`：让运行时输出到终端的中文不变成问号。
- `-cp`（classpath）：要同时包含编译输出目录 `/home/suzuha/hivejdbc` 和 Hive JDBC 独立 jar 包。

## 5. 代码讲解

```java
import java.sql.*;

public class HiveJDBCTest {
    // 驱动类名：Hive 的 JDBC 驱动
    private static final String DRIVER   = "org.apache.hive.jdbc.HiveDriver";
    // 连接 URL：jdbc:hive2://主机:端口/库名
    private static final String URL      = "jdbc:hive2://localhost:10000/db_20242093";
    private static final String USER     = "suzuha";
    private static final String PASSWORD = "";   // 空密码

    public static void main(String[] args) {
        Connection conn = null;
        Statement  stmt = null;
        ResultSet  rs   = null;
        try {
            // 1. 加载驱动
            Class.forName(DRIVER);
            System.out.println("驱动加载成功");

            // 2. 建立连接
            conn = DriverManager.getConnection(URL, USER, PASSWORD);
            System.out.println("数据库连接成功");

            // 3. 创建 Statement，开启本地模式
            stmt = conn.createStatement();
            stmt.execute("SET hive.exec.mode.local.auto=true");
            System.out.println("本地模式已开启\n");

            // 查询1：本人各科成绩
            System.out.println("===== 查询1：学号 20242093 的各科成绩 =====");
            String sql1 = "SELECT course, score FROM score_20242093 WHERE sid='20242093'";
            rs = stmt.executeQuery(sql1);
            while (rs.next()) {
                System.out.println("课程：" + rs.getString(1) + "    成绩：" + rs.getInt(2));
            }

            // 查询2：本人平均成绩（AVG 聚合，会触发 MapReduce）
            System.out.println("\n===== 查询2：学号 20242093 的平均成绩 =====");
            String sql2 = "SELECT AVG(score) FROM score_20242093 WHERE sid='20242093'";
            rs = stmt.executeQuery(sql2);
            if (rs.next()) {
                System.out.println("学号 20242093 平均成绩：" + rs.getDouble(1));
            }

            // 查询3：本人最高分记录（ORDER BY 触发 MapReduce）
            System.out.println("\n===== 查询3：学号 20242093 的最高分记录 =====");
            String sql3 = "SELECT id, sid, name, course, score FROM score_20242093 "
                        + "WHERE sid='20242093' ORDER BY score DESC LIMIT 1";
            rs = stmt.executeQuery(sql3);
            while (rs.next()) {
                System.out.println("序号：" + rs.getInt(1));
                System.out.println("学号：" + rs.getString(2));
                System.out.println("姓名：" + rs.getString(3));
                System.out.println("课程：" + rs.getString(4));
                System.out.println("成绩：" + rs.getInt(5));
            }
        } catch (ClassNotFoundException e) {
            System.out.println("驱动加载失败：" + e.getMessage());
            e.printStackTrace();
        } catch (SQLException e) {
            System.out.println("数据库操作失败：" + e.getMessage());
            e.printStackTrace();
        } finally {
            // 释放资源，顺序：rs → stmt → conn
            try { if (rs   != null) rs.close();   } catch (SQLException e) { e.printStackTrace(); }
            try { if (stmt != null) stmt.close(); } catch (SQLException e) { e.printStackTrace(); }
            try { if (conn != null) conn.close(); } catch (SQLException e) { e.printStackTrace(); }
            System.out.println("\n资源已全部关闭");
        }
    }
}
```

**逐段解释**：

| 代码段 | 作用 |
|---|---|
| `Class.forName(DRIVER)` | 把 Hive JDBC 驱动类加载进 JVM，否则 `DriverManager` 找不到驱动 |
| `DriverManager.getConnection(URL, USER, PASSWORD)` | 通过 URL 连到 HiveServer2 的 10000 端口，并指定使用 `db_20242093` 库 |
| `SET hive.exec.mode.local.auto=true` | 让聚合/排序查询在本地进程跑 MapReduce，不提交 YARN，避免单机卡死 |
| `rs.next()` | 结果集游标下移一行，有数据返回 true，无数据返回 false |
| `rs.getString(1)` / `rs.getInt(2)` | 按列序号取值（从 1 开始），类型要与 SQL 列类型匹配 |
| `finally` 块 | 无论是否异常都关闭资源，顺序与创建相反（rs → stmt → conn） |

**三个查询的 SQL 对照**：

| 查询 | SQL | 是否触发 MR |
|---|---|---|
| 查询1 各科成绩 | `SELECT course, score FROM ... WHERE sid='20242093'` | 否（简单扫描，秒出） |
| 查询2 平均分 | `SELECT AVG(score) FROM ... WHERE sid='20242093'` | 是（AVG 聚合） |
| 查询3 最高分 | `SELECT ... ORDER BY score DESC LIMIT 1` | 是（ORDER BY 排序） |

## 6. 截图对照说明

| 报告截图 | 内容转写 |
|---|---|
| 图2（Beeline 查询结果） | `SELECT * FROM score_20242093;` 返回 8 行，末尾显示 `8 rows selected (2.652 seconds)`，提示符 `0: jdbc:hive2://localhost:10000>`。前 6 行学号 20242093，后 2 行 20242094/20242095。 |
| 图3（Hadoop Web UI Overview） | 页面 `http://localhost:9870/dfshealth.html`，Hadoop 版本 **3.4.2**，`Overview 'localhost:9000' (active)`，Cluster ID、Block Pool ID 等信息。 |
| 图4（HDFS Browse Directory） | 路径 `/user/hive/warehouse/db_20242093.db/score_20242093`，文件 `score.txt`，大小 **315 B**，属主 `suzuha`，属组 `supergroup`，副本数 1，块大小 128 MB，修改时间 Oct 29 20:25。 |
| 图5（Java 运行输出上半） | 运行命令 `java -Dfile.encoding=UTF-8 -cp /home/suzuha/hivejdbc:/usr/local/hive-4.0.1/jdbc/hive-jdbc-4.0.1-standalone.jar HiveJDBCTest`；输出"驱动加载成功"；有 **SLF4J 警告**：`SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder"`（不影响功能）；"数据库连接成功""本地模式已开启"；查询1 的 6 条课程成绩。 |
| 图6（Java 运行输出下半） | 查询1 剩余成绩；查询2 `学号 20242093 平均成绩：88.0`；查询3 最高分（序号4、数据挖掘、95）；末尾"资源已全部关闭"。 |

> 注：报告中还有 1 张校徽装饰图（image1），不含操作信息，略。

## 7. 常见报错与排查

### 原文提到过的

| 问题 | 根因 | 解决 |
|---|---|---|
| 聚合查询（AVG/ORDER BY）无限期无响应 | 聚合/排序会触发 MapReduce，而 MR 默认提交给 YARN；单机环境下 YARN 资源不足导致作业挂起 | 执行 `SET hive.exec.mode.local.auto=true;` 开启本地模式，让 MR 在本地进程执行。验证：日志出现 `Job running in-process (local Hadoop)` 和 `Ended Job = job_local...`，Job ID 前缀由 `job_` 变为 `job_local`。开启后查询2 约 2.66 秒返回 88.0。 |

### 原文未明确提及、但截图可见或按环境推断的

| 问题 | 现象 | 处理 |
|---|---|---|
| SLF4J 警告 | 运行时打印 `SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder"` | 不影响功能，是日志绑定缺失的警告，可忽略；如需消除可加 slf4j 实现包。 |
| HiveServer2 启动失败 | 连不上 10000 端口 | 检查 HDFS 是否已启动（HiveServer2 依赖 HDFS）；检查 MySQL 元数据库是否正常。 |
| 导入后 `numRows=0` | 误以为导入失败 | 正常现象，看 `numFiles=1` 和 `totalSize` 是否匹配文件大小。 |
| 中文显示乱码/问号 | 终端或 JVM 编码不是 UTF-8 | 编译加 `-encoding UTF-8`，运行加 `-Dfile.encoding=UTF-8`。 |

> ⚠️ 报告中"问题8：实验中遇到了哪些坑"以及"参考示例代码的 4 处修正"两节**只有标题，未给出具体内容**（原文未说明）。

## 8. 自检清单

- [ ] `/home/suzuha/data/score.txt` 存在，8 行，315 字节
- [ ] Hive 中 `db_20242093.score_20242093` 表存在，列结构正确
- [ ] `LOAD DATA` 后 HDFS 表目录下有 `score.txt`，大小 315 B
- [ ] Beeline 中 `SELECT *` 返回 8 行，中文正常
- [ ] Hadoop Web UI 能看到 `/user/hive/warehouse/db_20242093.db/score_20242093/score.txt`
- [ ] Java 程序输出"驱动加载成功""数据库连接成功""本地模式已开启"
- [ ] 查询1 返回 6 行本人成绩（不含张三、李四）
- [ ] 查询2 平均分 = 88.0（验证：92+85+88+95+78+90 = 528，528/6 = 88.0）
- [ ] 查询3 最高分 = 数据挖掘 95 分
- [ ] 三个查询结果互相印证（6 个分数和为 528，平均 88.0，最大 95）
- [ ] 程序结束打印"资源已全部关闭"

## 9. 小结 + 易忘命令速记

**小结**：本实验把"人敲命令查 Hive"升级为"Java 程序自动查 Hive"。核心链路是：本地 CSV → `LOAD DATA` 搬进 HDFS → Beeline 验证 → Java JDBC 连 HiveServer2 执行查询。关键认识是 Hive 的元数据在 MySQL、数据在 HDFS、计算走 MapReduce；聚合/排序查询会触发 MR，单机环境需开本地模式避免卡死。

**易忘命令速记**：

```bash
# 建库建表
CREATE DATABASE db_20242093;
USE db_20242093;
CREATE TABLE score_20242093 (id INT, sid STRING, name STRING, course STRING, score INT)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;

# 导入
LOAD DATA LOCAL INPATH '/home/suzuha/data/score.txt' INTO TABLE score_20242093;

# Beeline 连接
beeline -u jdbc:hive2://localhost:10000/db_20242093

# 本地模式（防卡死）
SET hive.exec.mode.local.auto=true;

# 编译运行 Java
javac -encoding UTF-8 -cp /usr/local/hive-4.0.1/jdbc/hive-jdbc-4.0.1-standalone.jar -d /home/suzuha/hivejdbc /home/suzuha/hivejdbc/HiveJDBCTest.java
java -Dfile.encoding=UTF-8 -cp /home/suzuha/hivejdbc:/usr/local/hive-4.0.1/jdbc/hive-jdbc-4.0.1-standalone.jar HiveJDBCTest
```
