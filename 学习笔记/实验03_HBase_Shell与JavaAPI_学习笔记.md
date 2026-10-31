# 实验3：HBase Shell 操作与 Java API 实验 —— 学习笔记

> 本笔记根据实验报告 `20242093刘浩宇4.docx` 整理。报告原文以截图为主，
> 文中命令、输出均来自截图；**未在报告截图中出现的内容会明确标注"（原文未说明）"或"（参考同课资料补全）"**，绝不编造。

---

## 1. 实验在做什么 / 最终产出什么

**一句话**：在单机上安装部署 HBase 2.5.16（伪分布式，依赖 HDFS），用 **HBase Shell** 建一张学生成绩表并做增删查，再用 **Java API** 写程序读取该表并打印学生信息。

**最终产出**：
- 一个名为 `stu01` 的 HBase 表（含 `info`、`score` 两个列族），存了 3 条学生数据；
- 一个 `HBaseDemo.java` 程序，能按行键查询单个学生信息并扫描全表。

---

## 2. 环境与前置条件

| 项目 | 值 |
|---|---|
| 操作系统 | Linux（用户名 `suzuha`，主机名 `lhy`） |
| 部署模式 | 单机伪分布式 |
| Hadoop | 3.4.2（`/usr/local/hadoop-3.4.2`） |
| JDK | 11.0.2（`/usr/local/jdk-11.0.2`） |
| HBase | 2.5.16（`/usr/local/hbase-2.5.16`） |
| HDFS 地址 | `hdfs://localhost:9000`（来自 `core-site.xml` 的 `fs.defaultFS`） |

**启动顺序（重要，不能反）**：
1. 先启动 Hadoop：`start-dfs.sh`
2. 再启动 HBase：`start-hbase.sh`

---

## 3. 关键概念速览

| 术语 | 是什么 | 为什么需要 |
|---|---|---|
| **HBase** | 运行在 HDFS 上的面向列的 NoSQL 数据库 | 存储海量稀疏数据，按行键快速随机读写 |
| **RowKey（行键）** | 一行数据的唯一标识，按字典序排序 | HBase 只支持行键索引，查询必须靠它 |
| **列族 Column Family** | 建表时预先定义的列的分组，如 `info`、`score` | 物理上同列族数据存一起，影响存储和查询性能 |
| **列限定符 Column Qualifier** | 列族下的具体列，可动态增加，如 `info:name` | 不需要建表时定死所有列，灵活 |
| **单元格 Cell** | 由 RowKey + 列族 + 列限定符 + 时间戳 定位的值 | HBase 支持多版本，时间戳区分版本 |
| **ZooKeeper** | 分布式协调服务，HBase 用它管理 Master 选举、RegionServer 状态 | 没有它 HBase 无法协调各进程 |
| **HMaster** | HBase 主进程，负责建表、Region 分配 | 没有它无法进行 DDL（建表等） |
| **HRegionServer** | 实际存储和读写数据的进程 | 数据存在它上面 |

---

## 4. 完整操作步骤

### Step 1：启动 Hadoop（HBase 的数据要存在 HDFS 上）

```bash
start-dfs.sh
jps
```

**在干什么**：启动 HDFS 的 NameNode、DataNode、SecondaryNameNode。
**预期输出**（`jps` 能看到 3 个 Hadoop 进程）：
```
3527 NameNode
3722 DataNode
3941 SecondaryNameNode
4065 Jps
```
（对应报告 image5）

---

### Step 2：下载 HBase 安装包

```bash
cd ~
wget https://archive.apache.org/dist/hbase/2.5.16/hbase-2.5.16-bin.tar.gz
```

**在干什么**：从 Apache 归档站下载 HBase 2.5.16 二进制包（约 329M）。
**预期输出**：下载进度条到 100%，生成 `hbase-2.5.16-bin.tar.gz`。（对应报告 image6）

---

### Step 3：解压到 /usr/local 并改权限

```bash
sudo tar -zxf hbase-2.5.16-bin.tar.gz -C /usr/local
sudo chown -R suzuha:suzuha /usr/local/hbase-2.5.16
ls -ld /usr/local/hbase-2.5.16
```

**在干什么**：
- `tar -zxf`：解压到 `/usr/local`；
- `chown -R suzuha:suzuha`：把目录所有者改成当前用户，否则后面没权限写配置和日志。
**预期输出**：`ls -ld` 显示所有者为 `suzuha suzuha`。（对应报告 image2）

---

### Step 4：配置 ~/.bashrc 环境变量

```bash
vi ~/.bashrc
```

在文件末尾追加：

```bash
export JAVA_HOME=/usr/local/jdk-11.0.2
export PATH=$JAVA_HOME/bin:$PATH
export HADOOP_HOME=/usr/local/hadoop-3.4.2
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
export HBASE_HOME=/usr/local/hbase-2.5.16
export PATH=$HBASE_HOME/bin:$PATH
```

使配置生效并验证：

```bash
source ~/.bashrc
echo $HBASE_HOME        # 应输出 /usr/local/hbase-2.5.16
which hbase             # 应输出 /usr/local/hbase-2.5.16/bin/hbase
```

（对应报告 image3、image7）

---

### Step 5：配置 hbase-env.sh

```bash
vi /usr/local/hbase-2.5.16/conf/hbase-env.sh
```

修改/添加以下内容：

```bash
export JAVA_HOME=/usr/local/jdk-11.0.2
export HBASE_CLASSPATH=/usr/local/hadoop-3.4.2/etc/hadoop
export HBASE_MANAGES_ZK=true
export HBASE_DISABLE_HADOOP_CLASSPATH_LOOKUP=true
```

各行作用：
- `JAVA_HOME`：告诉 HBase 用哪个 JDK；
- `HBASE_CLASSPATH`：指向 Hadoop 配置目录，让 HBase 能找到 HDFS 地址；
- `HBASE_MANAGES_ZK=true`：用 HBase **自带的 ZooKeeper**（不需要单独装 ZK）；
- `HBASE_DISABLE_HADOOP_CLASSPATH_LOOKUP=true`：**避免 classpath 冲突导致的启动报错**（这个很关键，不加可能启动失败）。

（对应报告 image4）

---

### Step 6：配置 hbase-site.xml

> ⚠️ 报告截图中只显示了执行 `vi /usr/local/hbase-2.5.16/conf/hbase-site.xml`，
> **未贴出文件内容**。以下配置参考同课资料补全，核心属性与 HBase 2.5.16 一致。

先确认 HDFS 地址：

```bash
grep -A 2 "fs.defaultFS" /usr/local/hadoop-3.4.2/etc/hadoop/core-site.xml
```

报告中输出为：
```xml
<value>hdfs://localhost:9000</value>
```

然后编辑：

```bash
vi /usr/local/hbase-2.5.16/conf/hbase-site.xml
```

写入（路径中的版本号换成你自己的）：

```xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>hbase.cluster.distributed</name>
    <value>true</value>
  </property>
  <property>
    <name>hbase.tmp.dir</name>
    <value>./tmp</value>
  </property>
  <property>
    <name>hbase.unsafe.stream.capability.enforce</name>
    <value>true</value>
  </property>
  <property>
    <name>hbase.rootdir</name>
    <value>hdfs://localhost:9000/hbase</value>
  </property>
  <property>
    <name>hbase.zookeeper.property.dataDir</name>
    <value>/usr/local/hbase-2.5.16/zookeeper</value>
  </property>
</configuration>
```

（对应报告 image8、image12 中编辑该文件的动作；内容参考同课 HBase 手册）

---

### Step 7：启动 HBase 并验证进程

```bash
start-hbase.sh
jps
```

**预期输出**（`jps` 应能看到 3 个 H 开头的进程）：
```
7889 HMaster
8016 HRegionServer
7758 HQuorumPeer      ← HBase 自带的 ZooKeeper 进程
3941 SecondaryNameNode
3527 NameNode
3722 DataNode
8371 Jps
```

> 首次启动会出现 SSH 确认提示：
> ```
> The authenticity of host '127.0.0.1' can't be established.
> Are you sure you want to continue connecting (yes/no/[fingerprint])?
> ```
> 输入 `yes` 回车即可。（对应报告 image8、image9、image13）

---

### Step 8：HBase Shell 操作

进入 Shell：

```bash
hbase shell
```

#### 8.1 查看所有表

```hbase
list
```

**预期输出**（空表时）：
```
TABLE
0 row(s)
=> []
```
（对应报告 image14）

#### 8.2 建表

```hbase
create 'stu01', 'info', 'score'
```

**含义**：创建表 `stu01`，含两个列族 `info`（学生基本信息）和 `score`（成绩）。
**预期输出**：
```
Created table stu01
=> Hbase::Table - stu01
```
（对应报告 image15）

#### 8.3 查看表结构

```hbase
describe 'stu01'
```

**预期输出**：显示两个列族 `info`、`score` 及其属性（VERSIONS、TTL、BLOCKSIZE 等）。
（对应报告 image16）

#### 8.4 插入数据（put）

> put 语法：`put '表名', 'RowKey', '列族:列限定符', '值'`

学生 20242093（刘浩宇）：
```hbase
put 'stu01', '20242093', 'info:id', '20242093'
put 'stu01', '20242093', 'info:name', '刘浩宇'
put 'stu01', '20242093', 'info:hobby', '篮球'
put 'stu01', '20242093', 'score:bigdata', '95'
put 'stu01', '20242093', 'score:java', '88'
```

学生 20242094（李明）：
```hbase
put 'stu01', '20242094', 'info:name', '李明'
put 'stu01', '20242094', 'info:hobby', '读书'
put 'stu01', '20242094', 'score:bigdata', '87'
put 'stu01', '20242094', 'score:java', '92'
```

学生 20242095（王芳）：
```hbase
put 'stu01', '20242095', 'info:id', '20242095'
put 'stu01', '20242095', 'info:name', '王芳'
put 'stu01', '20242095', 'info:hobby', '音乐'
put 'stu01', '20242095', 'score:bigdata', '90'
put 'stu01', '20242095', 'score:java', '85'
```

（对应报告 image17、image18）

#### 8.5 查询单行（get）

查整行：
```hbase
get 'stu01', '20242093'
```

只查指定列：
```hbase
get 'stu01', '20242093', 'info:id', 'info:name'
```

> 注意：Shell 里中文值会显示成 `\xE5\x88\x98...` 这样的十六进制转义，不是乱码，是 HBase Shell 的正常显示方式。Java API 输出则是正常中文。
（对应报告 image19、image20）

#### 8.6 全表扫描（scan）

```hbase
scan 'stu01'
```

**预期输出**：3 行学生数据，每行按列展开。
（对应报告 image21）

#### 8.7 统计行数（count）

```hbase
count 'stu01'
```

**预期输出**：
```
3 row(s)
=> 3
```
（对应报告 image22）

#### 8.8 退出 Shell

```hbase
exit
```

---

### Step 9：Java API 操作

#### 9.1 编写 HBaseDemo.java

```bash
mkdir -p ~/hbasedemo
vi ~/hbasedemo/HBaseDemo.java
```

代码如下（与报告运行输出完全对应）：

```java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;

/**
 * 使用 HBase Java API 读取 stu01 表中指定学生的信息，并扫描全表
 */
public class HBaseDemo {

    public static void main(String[] args) throws IOException {
        // 1. 创建配置对象（自动加载 classpath 中的 hbase-site.xml）
        Configuration conf = HBaseConfiguration.create();

        // 2. 建立连接
        Connection conn = ConnectionFactory.createConnection(conf);

        // 3. 获取表对象
        Table table = conn.getTable(TableName.valueOf("stu01"));

        // 4. 按行键（学号）构造 Get 对象
        String rowKey = "20242093";
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = table.get(get);

        // 5. 取出各列的值（HBase 底层是字节数组，需要转成字符串）
        String id    = Bytes.toString(result.getValue(Bytes.toBytes("info"),  Bytes.toBytes("id")));
        String name  = Bytes.toString(result.getValue(Bytes.toBytes("info"),  Bytes.toBytes("name")));
        String hobby = Bytes.toString(result.getValue(Bytes.toBytes("info"),  Bytes.toBytes("hobby")));
        String bd    = Bytes.toString(result.getValue(Bytes.toBytes("score"), Bytes.toBytes("bigdata")));
        String java  = Bytes.toString(result.getValue(Bytes.toBytes("score"), Bytes.toBytes("java")));

        // 6. 输出
        System.out.println("========== 学生信息查询结果 ==========");
        System.out.println("行键(RowKey) : " + rowKey);
        System.out.println("学号         : " + id);
        System.out.println("姓名         : " + name);
        System.out.println("爱好         : " + hobby);
        System.out.println("大数据成绩   : " + bd);
        System.out.println("Java成绩     : " + java);
        System.out.println("=====================================");

        // 7. 扫描全表
        System.out.println("========== 全表数据 ==========");
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for (Result r : scanner) {
            String rk    = Bytes.toString(r.getRow());
            String rid   = Bytes.toString(r.getValue(Bytes.toBytes("info"), Bytes.toBytes("id")));
            String rname = Bytes.toString(r.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")));
            System.out.println(rk + "\t" + rid + "\t" + rname);
        }
        scanner.close();

        // 8. 关闭资源
        table.close();
        conn.close();
    }
}
```

> 报告截图只显示了编译和运行命令及输出，**未贴出源码**。以上代码来自同课《HBase Shell 与 Java API 实验全流程手册》，其输出与报告 image24 完全一致。

#### 9.2 编译

```bash
cd ~/hbasedemo
javac -cp $(hbase classpath) -d . HBaseDemo.java
```

**在干什么**：`hbase classpath` 会输出 HBase 的全部依赖 jar 路径，`$(...)` 把它展开；`-d .` 把编译生成的 `.class` 放在当前目录。

#### 9.3 运行

```bash
java -cp $(hbase classpath):. HBaseDemo
```

> 注意结尾的 `:.` 表示把当前目录也加入类路径，**不能省略**，否则找不到 `HBaseDemo.class`。

**预期输出**（对应报告 image24）：
```
========== 学生信息查询结果 ==========
行键(RowKey) : 20242093
学号         : 20242093
姓名         : 刘浩宇
爱好         : 篮球
大数据成绩   : 95
Java成绩     : 88
=====================================
========== 全表数据 ==========
20242093	20242093	刘浩宇
20242094	20242094	李明
20242095	20242095	王芳
```

> 运行时会有一些 SLF4J 和非法反射访问的 WARNING，不影响功能，可以忽略。

---

## 5. 代码讲解（HBaseDemo.java）

| 代码段 | 作用 |
|---|---|
| `HBaseConfiguration.create()` | 创建配置对象，自动加载 classpath 里的 `hbase-site.xml`，拿到 ZooKeeper 地址等 |
| `ConnectionFactory.createConnection(conf)` | 建立与 HBase 的连接（重量级对象，应复用） |
| `conn.getTable(TableName.valueOf("stu01"))` | 获取表对象，后续对表的操作都通过它 |
| `new Get(Bytes.toBytes(rowKey))` | 用行键构造查询条件。**HBase 一切皆字节数组**，所以字符串要 `Bytes.toBytes()` |
| `table.get(get)` | 执行查询，返回 `Result` 对象（包含该行所有列） |
| `result.getValue(列族字节, 列名字节)` | 取指定列的值，返回字节数组 |
| `Bytes.toString(...)` | 把字节数组还原成字符串 |
| `new Scan()` + `table.getScanner(scan)` | 构造全表扫描，得到 `ResultScanner`，遍历即逐行读取 |
| `table.close(); conn.close();` | 释放资源，避免连接泄漏 |

**核心理解**：HBase 中数据底层以**字节数组**存储，所以 API 里处处需要用 `Bytes.toBytes()` 把字符串转成字节数组传参，取回结果时再用 `Bytes.toString()` 转回来。

---

## 6. 截图对照说明

| 报告截图 | 内容 | 转写要点 |
|---|---|---|
| image2 | 解压 HBase + 改权限 | `tar -zxf`、`chown -R` |
| image3 | ~/.bashrc 环境变量 | JAVA_HOME / HADOOP_HOME / HBASE_HOME |
| image4 | hbase-env.sh 配置 | HBASE_MANAGES_ZK=true、HBASE_DISABLE_HADOOP_CLASSPATH_LOOKUP=true |
| image5 | start-dfs.sh + jps | NameNode/DataNode/SecondaryNameNode 起来 |
| image6 | wget 下载 HBase | 从 archive.apache.org 下载 2.5.16 |
| image8 | 查 fs.defaultFS + start-hbase.sh | HDFS 地址 hdfs://localhost:9000；首次 SSH 确认 |
| image9/image13 | HBase 启动成功 | jps 看到 HMaster/HRegionServer/HQuorumPeer |
| image10-12 | ZK 报错与修复 | `Zookeeper GET could not be completed` → 删 zk 数据 + 删 /hbase + 改配置 + 重启 |
| image14 | list 空表 | 0 row(s) |
| image15 | create 'stu01','info','score' | 建表成功 |
| image16 | describe 'stu01' | 查看列族结构 |
| image17-18 | put 插入 3 个学生 | 每个学生 5 个列 |
| image19-20 | get 查询 | 单行 / 指定列 |
| image21 | scan 全表 | 3 行数据 |
| image22 | count | 3 row(s) |
| image23 | javac + java 编译运行 | `$(hbase classpath)` 的用法 |
| image24 | Java 程序输出 | 学生信息 + 全表数据 |

---

## 7. 常见报错与排查

### 原文提到过的报错

**报错：`ERROR: Zookeeper GET could not be completed in 10000 ms`**
- 现象：`hbase shell` 里执行 `list` 报这个错（报告 image10、image11）。
- 原因：ZooKeeper 数据目录损坏或 hbase-site.xml 配置不对，导致 HBase 连不上 ZK。
- 报告中的解决步骤（image12）：
  ```bash
  stop-hbase.sh
  rm -rf /usr/local/hbase-2.5.16/zookeeper
  hadoop fs -rm -r /hbase
  vi /usr/local/hbase-2.5.16/conf/hbase-site.xml   # 检查/修正配置
  start-hbase.sh
  ```
  重启后 `jps` 看到 HQuorumPeer，再进 shell 就正常了。

### 按环境差异推断的报错

1. **`no hbase master found`**：执行 `stop-hbase.sh` 时出现，通常是 HMaster 没正常启动。先 `jps` 看进程，再检查日志 `/usr/local/hbase-2.5.16/logs/`。
2. **SSH 首次连接确认**：`start-hbase.sh` 会通过 SSH 连本机，首次需要输 `yes`。如果想免密可配置 `ssh localhost` 免密登录。
3. **`javac: 无效的目标发行版`**：JDK 版本不匹配，可显式指定：`javac -source 11 -target 11 -cp $(hbase classpath) -d . HBaseDemo.java`。
4. **运行 Java 时 `ClassNotFoundException: HBaseDemo`**：忘了加 `:.`（当前目录），类路径里没有 `.class` 所在目录。

---

## 8. 自检清单

| 检查项 | 命令 / 方法 | 成功标志 |
|---|---|---|
| HDFS 启动 | `jps` | 有 NameNode、DataNode、SecondaryNameNode |
| HBase 安装路径正确 | `echo $HBASE_HOME` | 输出 `/usr/local/hbase-2.5.16` |
| HBase 命令可用 | `which hbase` | 输出 `/usr/local/hbase-2.5.16/bin/hbase` |
| HBase 启动 | `jps` | 有 HMaster、HRegionServer、HQuorumPeer |
| Shell 能连上 | `hbase shell` → `list` | 不报错，空表返回 `0 row(s)` |
| 建表成功 | `list` | 显示 `stu01` |
| 数据插入成功 | `count 'stu01'` | 返回 `3` |
| Java 编译成功 | `ls *.class` | 有 `HBaseDemo.class` |
| Java 运行成功 | `java -cp $(hbase classpath):. HBaseDemo` | 打印出 3 个学生信息 |

---

## 9. 小结 + 易忘命令速记

**一句话小结**：HBase 跑在 HDFS 上，靠行键定位数据；Shell 用 `create/put/get/scan/count`，Java API 用 `HBaseConfiguration → Connection → Table → Get/Scan`，记住"一切皆字节数组"。

**易忘命令速记**：
```bash
# 启停
start-dfs.sh && start-hbase.sh
stop-hbase.sh && stop-dfs.sh

# Shell 常用
hbase shell
list
create '表名', '列族1', '列族2'
put '表名', 'RowKey', '列族:列', '值'
get '表名', 'RowKey'
scan '表名'
count '表名'
describe '表名'

# Java 编译运行（注意 $(hbase classpath) 和 :.）
javac -cp $(hbase classpath) -d . HBaseDemo.java
java -cp $(hbase classpath):. HBaseDemo
```
