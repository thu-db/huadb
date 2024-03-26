# HuaDB

HuaDB 为清华大学数据库内核课程的实验框架，实验说明可参阅[课程文档](https://thu-db.github.io/huadb-doc/)，课程资料可在[课程主页](https://dbgroup.cs.tsinghua.edu.cn/ligl/courses_cn.html)查阅。

本课程于每年春季学期进行，最新版本为 2024 版。

如果在实验中遇到任何问题，可以在 [Discussions](https://github.com/thu-db/huadb/discussions) 中讨论。如果发现了实验框架的 bug 可以提交 [Issue](https://github.com/thu-db/huadb/issues) 或 [Pull request](https://github.com/thu-db/huadb/pulls)。

## 编译与测试

运行如下命令进行编译：

```bash
make lab1-debug
```

你也可以通过设置 CMAKE_BUILD_PARALLEL_LEVEL 环境变量来并行编译：

```bash
CMAKE_BUILD_PARALLEL_LEVEL=$(nproc) make lab1-debug
```

运行如下命令进入数据库交互界面：

```bash
make shell
```

## 代码结构

实验框架主要包含以下几个模块，加粗部分为实验中涉及到的、需要你来补充的模块：

-   binder: 语义解析模块
-   catalog: 系统表模块
-   common: 工具模块，包含字符串处理函数、异常相关类等
-   database: 数据库引擎
-   **executors: 查询执行模块**
-   **log: 日志模块**
-   operators: 查询计划树节点
-   **optimizer: 优化器**
-   planner: 查询计划生成模块
-   **storage: 存储模块**
-   **table: 表相关类及函数**
-   **transaction: 事务模块**

## 第三方库

本项目包含了如下第三方库代码：

- [argparse](https://github.com/p-ranav/argparse)：使用 C++17 编写的命令行解析器，可以轻松定义用户友好的命令行参数
- [fmt](https://github.com/fmtlib/fmt): 提供了类似 Python 的字符串插值功能，可以方便地将变量插入到字符串中
- [libfort](https://github.com/seleznevae/libfort): 用于在终端输出表格，提供了表格布局和样式设置的功能
- [libpg_query](https://github.com/duckdb/duckdb/tree/main/third_party/libpg_query): 由 DuckDB 改写的支持 PostgreSQL 语法的解析器，用于查询解析
- [linenoise](https://github.com/antirez/linenoise): 用于创建交互式命令行界面，简化了用户输入的处理和历史记录的管理

## 致谢

在 HuaDB 的设计和开发过程中，我们参考了如下优秀的开源数据库项目，对他们的工作表示感谢：

- [BusTub](https://github.com/cmu-db/bustub): The BusTub Relational Database Management System (Educational)
- [DuckDB](https://github.com/duckdb/duckdb): An in-process SQL OLAP Database Management System
- [PostgreSQL](https://github.com/postgres/postgres): The World's Most Advanced Open Source Relational Database
