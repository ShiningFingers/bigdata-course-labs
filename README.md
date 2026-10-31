# 大数据系统基础 实验报告与学习笔记

《大数据系统基础》课程 8 个实验的实验报告（docx）与配套学习笔记（Markdown），覆盖 Hadoop、MapReduce、HBase、Hive、Spark 等主流大数据组件的安装部署、Shell 操作与编程实践。

## 环境版本

| 组件 | 版本 |
| --- | --- |
| Hadoop | 3.4.2 |
| JDK | 11.0.2 |
| HBase | 2.5.16 |
| Hive | 4.0.1 |
| MySQL | 8.0 |
| Spark | 3.5.9 |

## 目录结构

```
bigdata-course-labs/
├── 实验报告/        # 8 份实验报告原文（docx）
├── 学习笔记/        # 8 份配套学习笔记（Markdown）
├── README.md
└── .gitignore
```

- `实验报告/`：按 `实验NN_主题.docx` 命名，对应课程提交的原始 docx 报告。
- `学习笔记/`：按 `实验NN_主题_学习笔记.md` 命名，与报告一一对应，便于速查与复盘。

## 实验索引

| 实验 | 主题 | 技术栈 | 报告 | 笔记 |
| --- | --- | --- | --- | --- |
| 实验01 | Hadoop 安装与 Shell 操作 | Hadoop / HDFS Shell / MapReduce WordCount | [实验报告](实验报告/实验01_Hadoop安装与Shell操作.docx) | [学习笔记](学习笔记/实验01_Hadoop安装与Shell操作_学习笔记.md) |
| 实验02 | MapReduce 买家收藏统计 | MapReduce / Java / Maven | [实验报告](实验报告/实验02_MapReduce买家收藏统计.docx) | [学习笔记](学习笔记/实验02_MapReduce买家收藏统计_学习笔记.md) |
| 实验03 | HBase Shell 与 Java API | HBase / Shell / Java API | [实验报告](实验报告/实验03_HBase_Shell与JavaAPI.docx) | [学习笔记](学习笔记/实验03_HBase_Shell与JavaAPI_学习笔记.md) |
| 实验04 | MapReduce 去重/求平均/排序/Top5/分区 | MapReduce / Java | [实验报告](实验报告/实验04_MapReduce去重求平均排序Top5分区.docx) | [学习笔记](学习笔记/实验04_MapReduce去重求平均排序Top5分区_学习笔记.md) |
| 实验05 | 互联网精准广告推送（TF-IDF） | MapReduce / TF-IDF | [实验报告](实验报告/实验05_互联网精准广告推送TFIDF.docx) | [学习笔记](学习笔记/实验05_互联网精准广告推送TFIDF_学习笔记.md) |
| 实验06 | Hive 安装部署与 WordCount | Hive / HQL | [实验报告](实验报告/实验06_Hive安装部署与WordCount.docx) | [学习笔记](学习笔记/实验06_Hive安装部署与WordCount_学习笔记.md) |
| 实验07 | Hive JDBC 操作 | Hive / JDBC / Java | [实验报告](实验报告/实验07_HiveJDBC操作.docx) | [学习笔记](学习笔记/实验07_HiveJDBC操作_学习笔记.md) |
| 实验08 | Spark 安装与 WordCount | Spark / Scala / WordCount | [实验报告](实验报告/实验08_Spark安装与WordCount.docx) | [学习笔记](学习笔记/实验08_Spark安装与WordCount_学习笔记.md) |

## 怎么用这份笔记

1. **先看概念速览**：每份笔记开头通常有组件定位、核心概念与运行流程的快速梳理，帮助建立整体认知。
2. **照操作步骤敲**：按照笔记中的命令与代码示例在本地环境逐步复现实验，遇到报错先对照「排错」小节定位。
3. **排错节兜底**：笔记末尾的排错记录汇总了常见问题（端口占用、版本不兼容、路径错误等）与解决办法，遇到同类问题可直接参考。

建议按实验序号顺序阅读，后续实验依赖前序实验的环境与组件配置。

## 说明

- 学习笔记由实验报告转写整理而成，旨在提炼可复用的操作步骤与知识点，便于快速复习。
- 报告原文中缺失的配置细节或源码片段，已在笔记对应位置标注 **「（原文未说明）」**，复现实验时需结合官方文档自行补齐。
- 报告为课程提交时的原始 docx，未作内容修改；如需修正请直接更新对应笔记。

## 后续增补

新增实验时，在 `实验报告/` 与 `学习笔记/` 下分别放入 `实验NN_主题.docx` 与 `实验NN_主题_学习笔记.md`，并在上方「实验索引」表中追加一行即可。
