# 实验1：Hadoop 安装、Shell 操作实验 — 学习笔记

> 面向零基础，照着命令做就能跑通。本文根据实验报告原文转写，命令/路径/输出均来自原文，未出现的细节标注"（原文未说明）"。

---

## 1. 这个实验在做什么

从零搭建一套 **Hadoop 伪分布式环境**（单机模拟分布式），并在 HDFS 上完成目录创建、文件上传、词频统计（WordCount）、安全模式管理等基本操作，最终产出一个能正常运行 `jps` 看到 5 个守护进程、能跑通官方 WordCount 示例的 Hadoop 环境。

---

## 2. 环境与前置条件

| 项 | 值 |
|---|---|
| 操作系统 | Ubuntu 虚拟机（通过 Xshell 从 Windows 远程连接） |
| 虚拟机 IP | 192.168.88.129 |
| 主机名 | lhy |
| JDK | 11.0.2（Hadoop 3.4.2 兼容 JDK 11，原文验证可正常运行） |
| Hadoop | 3.4.2 |
| 安装目录 | /usr/local/hadoop-3.4.2 |
| 编辑器 | vim（需自行安装） |
| 传输工具 | Xshell + Xftp（Windows → 虚拟机） |

**前置准备**：
- Ubuntu 上安装 SSH 服务端、关闭防火墙
- Windows 端下载 JDK 11.0.2 和 Hadoop 3.4.2 压缩包，通过 Xftp 传到虚拟机

---

## 3. 关键概念速览

1. **Hadoop 三种运行模式**
   - **本地模式（Standalone）**：默认模式，所有进程跑在单个 JVM 里，用本地文件系统，主要用于调试。
   - **伪分布模式（Pseudo-Distributed）**：单台机器上每个守护进程（NameNode、DataNode 等）各跑一个独立 JVM，数据存 HDFS。本次实验用的就是这个。
   - **完全分布模式（Fully-Distributed）**：多台物理/虚拟节点组成真正的集群，用于生产。

2. **HDFS**：Hadoop 分布式文件系统。NameNode 管元数据（文件目录树），DataNode 存实际数据块，SecondaryNameNode 做 checkpoint。

3. **SSH 免密登录**：Hadoop 启动脚本要通过 SSH 登录本机启动各守护进程，必须配置 `ssh localhost` 免密。

4. **`jps`**：JDK 自带的 Java 进程查看工具，用来确认 Hadoop 守护进程是否启动成功。

5. **`hadoop fs`**：HDFS 的 Shell 命令入口，子命令和 Linux 命令高度相似（`-mkdir`、`-ls`、`-put` 等）。

6. **安全模式（Safe Mode）**：HDFS 启动时检查数据块完整性的只读保护状态，期间只能读不能写。

7. **MapReduce / WordCount**：Hadoop 自带的示例程序，统计文本中每个单词出现的次数，用来验证环境是否正常。

---

## 4. 完整操作步骤

### Part 1：Hadoop 伪分布模式安装

#### Step 1：安装 SSH 服务端 + 查 IP + 关防火墙

```bash
# 安装 SSH 服务端（此时还需要输入密码）
sudo apt-get install openssh-server

# 查 IP（得到 192.168.88.129）
ip addr   # 或 ifconfig

# 关闭防火墙
sudo ufw disable
```

**预期**：Windows 上用 Xshell 能连上 192.168.88.129。

#### Step 2：安装 JDK 11.0.2

1. Windows 下载 JDK 11.0.2 压缩包，用 Xftp 传到虚拟机。
2. 解压到指定目录（原文未说明具体路径，通常解压到 /usr/local/jdk-11.0.2）。
3. 配置环境变量，在 `/etc/profile` 末尾添加：

```bash
export JAVA_HOME=/usr/local/jdk-11.0.2   # 路径按实际解压位置
export PATH=$JAVA_HOME/bin:$PATH
```

```bash
# 使环境变量生效
source /etc/profile

# 验证
java -version
```

**预期**：`java -version` 显示 11.0.2。

> vim 小提示：按 `i` 进入插入模式编辑，按 `Esc` 回到命令模式，输入 `:wq` 保存退出，`:q!` 强制退出不保存。

#### Step 3：安装 vim（系统未预装）

```bash
sudo apt-get install vim
```

> 坑：vim 非正常退出会生成 `.swp` 隐藏文件，下次打开报警告。解决：`rm .xxx.swp` 删掉对应交换文件。

#### Step 4：解压 Hadoop

```bash
# 把 Hadoop 压缩包传到虚拟机后解压
tar -zxvf hadoop-3.4.2.tar.gz -C /usr/local/

# 改权限（按需）
chmod -R 755 /usr/local/hadoop-3.4.2
```

> 坑：原文提到压缩包文件名后缀与实际格式不匹配导致解压失败，修正后缀后成功。可用 `file 文件名` 检查真实格式。

#### Step 5：配置 SSH 免密登录

```bash
# 生成密钥（一路回车）
ssh-keygen -t rsa

# 把公钥加入授权列表
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys

# 测试免密登录
ssh localhost
ssh lhy
```

**预期**：`ssh localhost` 和 `ssh lhy` 都不需要密码。

#### Step 6：配置 Hadoop 环境变量

在 `/etc/profile` 末尾追加：

```bash
export HADOOP_HOME=/usr/local/hadoop-3.4.2
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
```

```bash
source /etc/profile
```

> 坑：执行 `hdfs` 命令提示 `command not found`，就是因为 bin 目录没加到 PATH，加完 `source /etc/profile` 即可。

#### Step 7：修改 core-site.xml

文件路径：`/usr/local/hadoop-3.4.2/etc/hadoop/core-site.xml`

```xml
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
</configuration>
```

> 说明：指定 HDFS 的默认访问地址。具体 property 内容原文未完整列出，以上为标准配置，以实际环境为准。

#### Step 8：修改 hdfs-site.xml

文件路径：`/usr/local/hadoop-3.4.2/etc/hadoop/hdfs-site.xml`

```xml
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
</configuration>
```

> 伪分布式只有 1 个 DataNode，副本数设为 1。具体 property 内容原文未完整列出，以上为标准配置。

> 坑：文件权限是只读时，先 `chmod +w 文件名` 加写权限；权限不足用 `sudo`。

#### Step 9：格式化 NameNode

```bash
hdfs namenode -format
```

**预期**：输出 `successfully formatted`。

> ⚠️ 警告：重复格式化会清空 HDFS 上所有数据，配置确认无误后再执行。

#### Step 10：启动 Hadoop

```bash
cd /usr/local/hadoop-3.4.2/sbin
./start-all.sh
```

启动日志依次提示：
```
Starting namenodes on [localhost]
Starting datanodes
Starting secondary namenodes [lhy]
Starting resourcemanager
Starting nodemanagers
```

#### Step 11：用 jps 验证进程

```bash
jps
```

**预期输出**（5 个 Java 进程）：
```
NameNode
DataNode
SecondaryNameNode
ResourceManager
NodeManager
```

> 坑：如果 NameNode/DataNode 起不来，多半是 core-site.xml 或 hdfs-site.xml 配置错误，看 Hadoop 日志定位。修改配置后一般重启即可，**不要重复格式化**。

---

### Part 2：Hadoop Shell 基本操作

#### Step 12：HDFS 常用命令练习

```bash
# 创建目录
hadoop fs -mkdir /test

# 创建空文件
hadoop fs -touchz /test/empty.txt

# 查看目录
hadoop fs -ls /
hadoop fs -ls -R /test     # 递归查看

# 移动 / 重命名
hadoop fs -mv /test/empty.txt /test/renamed.txt

# 复制
hadoop fs -cp /test/renamed.txt /test/copy.txt

# 上传本地文件到 HDFS
hadoop fs -put 本地文件路径 /目标路径

# 查看文件内容
hadoop fs -cat /test/xxx
hadoop fs -tail /test/xxx

# 查看文件大小
hadoop fs -du /test

# 以文本格式输出
hadoop fs -text /test/xxx

# 查看统计信息
hadoop fs -stat /test/xxx

# 下载到本地
hadoop fs -get /test/xxx 本地路径

# 修改拥有者 / 权限
hadoop fs -chown 用户名 /test/xxx
hadoop fs -chmod 755 /test/xxx

# 删除文件 / 递归删除目录
hadoop fs -rm /test/xxx
hadoop fs -rm -r /test

# 清空回收站
hadoop fs -expunge
```

> 说明：`hadoop fs` 和 `hdfs dfs` 用法基本一致，两者都可用。

#### Step 13：运行官方 WordCount 示例

```bash
# 1. 本地创建数据文件
mkdir -p /data
echo "hello world, Hadoop, ipieuvre" > /data/data.txt

# 2. 上传到 HDFS
hadoop fs -mkdir /in
hadoop fs -put /data/data.txt /in

# 3. 运行 WordCount
hadoop jar /usr/local/hadoop-3.4.2/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.4.2.jar wordcount /in /out

# 4. 查看结果
hadoop fs -cat /out/*
```

**预期输出**（每个单词词频均为 1）：
```
Hadoop,    1
hello      1
ipieuvre   1
world,     1
```

> 注意：官方示例只按空白字符切分，所以 `world,`、`Hadoop,` 带标点，同一个词因标点不同会被统计成不同的词（见 Step 14 改进）。

#### Step 14：WordCount 分词改进（自行编写 MapReduce）

**问题**：官方版输出 `world,`、`hadoop,`、`indeed.` 等带标点的脏词，且 `Hello` 和 `hello` 被当成两个词。

**改进思路**：
1. 用正则 `[^a-zA-Z0-9]+` 作分隔符，剥离所有非字母数字字符；
2. 分词前 `toLowerCase()` 统一转小写。

**Mapper 核心代码**（Java）：

```java
protected void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
    String line = value.toString().toLowerCase();
    String[] tokens = line.split("[^a-zA-Z0-9]+");
    for (String token : tokens) {
        if (!token.isEmpty()) {
            word.set(token);
            context.write(word, one);
        }
    }
}
```

**流程**：IDEA 建 Maven 项目 → 引入 `hadoop-client 3.4.2`（scope=provided）→ 写 Mapper/Reducer/Driver → `mvn package` 打 jar → `scp` 传到虚拟机 → `hadoop jar` 提交运行。

**改进后结果**：所有标点被剥离，单词正确切分，词频统计准确。

#### Step 15：安全模式操作

```bash
# 进入安全模式（HDFS 变为只读）
hdfs dfsadmin -safemode enter

# 退出安全模式
hdfs dfsadmin -safemode leave
```

**预期**：进入安全模式后写入操作会被拒绝；退出后恢复正常。

#### Step 16：按要求创建学号目录和文件

```bash
# 在 HDFS 根目录创建以学号命名的文件夹
hadoop fs -mkdir /20242093

# 本地创建 student.txt，内容为：
# SNO:20242093 SNAME:刘浩宇
echo "SNO:20242093 SNAME:刘浩宇" > /data/student.txt

# 上传并查看
hadoop fs -put /data/student.txt /20242093
hadoop fs -cat /20242093/student.txt
```

**预期输出**：
```
SNO:20242093 SNAME:刘浩宇
```

#### Step 17：关闭 Hadoop

```bash
cd /usr/local/hadoop-3.4.2/sbin
./stop-all.sh
```

> 注意：实验中通过 Xshell 连接虚拟机（第一层 SSH），虚拟机内又 `ssh localhost`（第二层），可能还有 `su` 切用户（第三层），所以退出时需要连续三次 `exit` 才能回到本地 shell。

---

## 5. 代码讲解

### 5.1 改进版 WordCount Mapper

```java
protected void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
    // 1. 转小写，避免 Hello 和 hello 被当成两个词
    String line = value.toString().toLowerCase();
    // 2. 用非字母数字字符作分隔符，标点全被剥离
    String[] tokens = line.split("[^a-zA-Z0-9]+");
    for (String token : tokens) {
        if (!token.isEmpty()) {       // 跳过空串（行首/连续分隔符会产生）
            word.set(token);
            context.write(word, one); // 输出 (单词, 1)
        }
    }
}
```

- `key`：该行在文件中的字节偏移量（用不到）。
- `value`：该行的文本内容。
- `context.write(word, one)`：把中间结果写给框架，由 Shuffle 阶段按 key 分组。

### 5.2 官方版 vs 改进版对比

| 维度 | 官方版 | 改进版 |
|---|---|---|
| 分词方式 | 按空白字符切分 | 按非字母数字字符切分 |
| 大小写 | 不处理，大小写算不同词 | 统一转小写 |
| 标点 | 粘连在单词上（`world,`） | 全部剥离 |
| 统计准确性 | 低，同一词重复统计 | 高，统计准确 |

---

## 6. 截图对照说明

> 原文截图共 46 张，以下挑与操作步骤相关的关键截图转写。截图编号按文档出现顺序。

- **第 2–4 张图**：Ubuntu 安装 SSH 服务端、查 IP（192.168.88.129）、关闭防火墙。
- **第 5–6 张图**：Xshell 成功连接到 Ubuntu 虚拟机；Xftp 传输 JDK 和 Hadoop 压缩包。
- **第 7–9 张图**：解压 JDK、安装 vim、编辑 `/etc/profile` 配置 `JAVA_HOME`。
- **第 10–12 张图**：解压 Hadoop，`ls -l` 看到目录权限 `drwxr-xr-x`（755）。
- **第 13–15 张图**：`ssh-keygen` 生成密钥、`cat id_rsa.pub >> authorized_keys`、`ssh localhost` 免密测试成功。
- **第 16–20 张图**：修改 `core-site.xml`、`hdfs-site.xml`；处理只读权限（`chmod +w`）；删除 `.swp` 交换文件。
- **第 21–23 张图**：`hdfs namenode -format` 成功；`./start-all.sh` 启动；`jps` 看到 NameNode、DataNode、SecondaryNameNode、ResourceManager、NodeManager 五个进程。
- **第 24–30 张图**：`hadoop fs` 系列命令操作（`-mkdir`、`-touchz`、`-ls`、`-ls -R`、`-mv`、`-cp`、`-put`、`-cat`、`-tail`、`-du`、`-text`、`-stat`、`-get`、`-chown`、`-chmod`、`-rm`、`-rm -r`、`-expunge`）。
- **第 31–35 张图**：WordCount 运行过程，`hadoop jar ... wordcount /in /out`，`hadoop fs -cat /out/*` 查看词频结果。
- **第 36–40 张图**：改进版 WordCount 的 IDEA 代码、Maven 打包、scp 传输、提交运行，对比官方版与改进版输出差异。
- **第 41–43 张图**：`hdfs dfsadmin -safemode enter` / `leave` 安全模式操作。
- **第 44–46 张图**：`hadoop fs -mkdir /20242093`、上传 `student.txt`、`-cat` 查看内容；关闭 Hadoop。

---

## 7. 常见报错与排查

### 原文提到过的问题

| 问题 | 原因 | 解决 |
|---|---|---|
| Hadoop 压缩包解压失败 | 文件名后缀与实际格式不匹配 | 用 `file` 检查真实格式，修正后缀后再解压 |
| vim 打开文件报 `.swp` 警告 | 上次非正常退出留下交换文件 | `rm .xxx.swp` 删除对应交换文件 |
| `hdfs: command not found` | Hadoop bin 目录没加到 PATH | 在 `/etc/profile` 加 `PATH=$HADOOP_HOME/bin:$PATH`，`source /etc/profile` |
| `Permission denied` | 目录权限不足 | `chmod` 改权限或 `chown` 改所有者，或用 `sudo` |
| NameNode/DataNode 启动失败 | `core-site.xml` / `hdfs-site.xml` 参数或路径写错 | 查看 Hadoop 日志定位错误，修正配置后重启；**不要重复格式化** |
| `jps` 看不到 Hadoop 进程 | 环境变量未在当前 shell 生效 | `source /etc/profile` 或重开终端 |

### 按环境差异推断的问题

| 问题 | 可能原因 | 排查方向 |
|---|---|---|
| `ssh localhost` 仍要密码 | `authorized_keys` 权限不对（应为 600）或 `.ssh` 目录权限不对（应为 700） | `chmod 700 ~/.ssh && chmod 600 ~/.ssh/authorized_keys` |
| NameNode 格式化后 DataNode 起不来 | DataNode 的 clusterID 和 NameNode 不一致（重复格式化导致） | 删除 `hdfs-site.xml` 中 `dfs.datanode.data.dir` 指定目录下的 current 文件夹，重启 DataNode |
| 9000 端口连不上 | core-site.xml 中 `fs.defaultFS` 配置错或 NameNode 没起来 | `jps` 确认 NameNode 进程存在；检查 core-site.xml |

---

## 8. 自检清单

- [ ] `java -version` 显示 11.0.2
- [ ] `ssh localhost` 和 `ssh lhy` 免密登录成功
- [ ] `hdfs namenode -format` 输出 `successfully formatted`
- [ ] `./start-all.sh` 后 `jps` 能看到 5 个进程：NameNode、DataNode、SecondaryNameNode、ResourceManager、NodeManager
- [ ] `hadoop fs -ls /` 能正常列出 HDFS 根目录
- [ ] WordCount 示例运行成功，`hadoop fs -cat /out/*` 有词频输出
- [ ] `hdfs dfsadmin -safemode enter` 后写入被拒绝，`leave` 后恢复
- [ ] `hadoop fs -cat /20242093/student.txt` 输出 `SNO:20242093 SNAME:刘浩宇`

---

## 9. 小结 + 易忘命令速记

**小结**：本实验从零搭建 Hadoop 伪分布式环境，核心链路是「装 JDK → 配 SSH 免密 → 解压 Hadoop → 改 core-site/hdfs-site → 格式化 NameNode → start-all.sh → jps 验证」，然后用 `hadoop fs` 系列命令操作 HDFS，用官方 WordCount 验证环境，最后用自写 Mapper 改进分词逻辑。

**易忘命令速记**：

```bash
# 启动/关闭
cd /usr/local/hadoop-3.4.2/sbin
./start-all.sh        # 启动全部
./stop-all.sh         # 关闭全部
jps                   # 看 Java 进程

# HDFS 常用
hadoop fs -mkdir /xxx          # 建目录
hadoop fs -put 本地 /hdfs      # 上传
hadoop fs -cat /hdfs/xxx       # 看内容
hadoop fs -rm -r /hdfs/xxx     # 递归删
hadoop fs -ls /                # 列目录

# WordCount
hadoop jar .../hadoop-mapreduce-examples-3.4.2.jar wordcount /in /out

# 安全模式
hdfs dfsadmin -safemode enter
hdfs dfsadmin -safemode leave

# 环境变量生效
source /etc/profile
```
