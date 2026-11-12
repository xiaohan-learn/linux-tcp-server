# Linux TCP 多线程服务器

一个从零实现的 Linux 网络服务器项目。不追求"一次写个完美服务器"，而是**同一需求下逐步演进架构**，体会不同并发模型的取舍——这正是后端 / Linux 开发面试最喜欢追问的脉络。

## 项目定位：架构演进主线

| 版本 | 并发模型 | 并发能力 | 状态 |
|------|----------|----------|------|
| v1 | 阻塞单连接（回声） | 0（一次只服务一个客户端） | ✅ 已完成 |
| v2 | 多线程（每连接一线程 · 聊天室） | 高（每连接独立线程） | ✅ 已完成 |
| v3 | 线程池（预创建 worker + 任务队列） | 高（可控线程数，避免频繁建线程） | ✅ 已完成 |
| v4 | epoll + 线程池（IO 多路复用） | 很高（单线程管上万 fd） | ✅ 已完成 |

演进解决的核心问题：v1 一次只能服务一个客户端 → v2 用多线程让连接并发 → v3 用线程池避免"每连接建线程"的开销与失控 → v4 用 epoll 把 IO 等待与业务处理解耦，支撑海量连接。

## 已实现功能

### v1 回声服务器
- 完整的 socket 流程：`socket → bind → listen → accept → recv/send`
- 单客户端回声；`SO_REUSEADDR` 端口复用、`EINTR` 重试、循环 `send` 处理短写

### v2 多线程聊天室
- 每连接一线程（`pthread_create` + `pthread_detach` 自回收，不堆积僵尸线程）
- 全局客户端表 `g_clients[]` 用 `pthread_mutex_t` 保护，线程安全
- 群发 `broadcast`：任意客户端消息转发给其他所有在线客户端
- 线程间传参用 `malloc` 拷值（避免栈变量竞争），线程内 `free`
- 忽略 `SIGPIPE`，避免对端断开时进程被信号杀死
- 统一的"单一清理点"结构，避免 fd 重复关闭 / 泄漏

### v3 线程池回声服务器
- 主线程 `accept` 收连接 → `malloc` 拷 fd → 投入**任务队列**（`int* queue[]` 环形队列）
- 预创建 `WORKER_NUM=4` 个常驻 worker 线程，通过 `pthread_cond_wait` 等任务、取到即处理，避免"每连接建线程"的开销与失控
- 互斥锁 `pthread_mutex_t` + 条件变量 `pthread_cond_t` 协作：主线程入队后 `cond_signal` 唤醒一个 worker
- 经典坑全规避：`cond_wait` 用 `while` 防虚假唤醒、持锁只包"取/放任务"不包 IO、fd 用 `malloc` 传参、退出用 `broadcast` + `join`
- `SIGPIPE` 忽略、循环 `recv/send` 短读写处理

### v4 epoll + 线程池服务器
- 主线程跑 **epoll 事件循环**一处盯所有 fd：`listenfd` 用 `EPOLLIN`（LT）注册，`clientfd` 用 `EPOLLIN | EPOLLONESHOT` 注册
- **IO 等待与业务处理解耦**：client 可读时主线程只把 fd 投入任务队列（自己不读数据），worker 取到后非阻塞 `recv` 循环读到 `EAGAIN` → `send` 回声
- **`EPOLLONESHOT` 防竞态**：fd 触发一次后自动关闭通知，worker 处理完用 `EPOLL_CTL_MOD` **重新 ARM**，保证同一时刻只有一个 worker 动这个 fd
- v3→v4 本质区别：v3 的 worker 拿到 fd 后**阻塞 `recv`** 占住线程（4 个 worker 同时只能服务 4 个"正在 recv"的连接）；v4 把 IO 等待交给 epoll，**少量线程即可扛海量连接**
- 关键坑全规避：非阻塞 fd 必须循环读到 `EAGAIN` 才结束本轮、`EPOLLONESHOT` 必须重新 ARM（否则 fd 假死）、`epfd` 设为全局供 worker `epoll_ctl`、持锁只包"取/放任务"不包 IO、send 出错 `DEL+close` 防 fd 泄漏、recv 区分 `EINTR` 重试

## 编译运行

需要 Linux 环境（gcc + pthread，已在 WSL2 Ubuntu 验证）。

```bash
make                          # 编译 v1、v2、v3、v4
./v1_echo_server              # 启动回声服务器
./v2_chat_server              # 启动聊天室服务器
./v3_thread_pool_server       # 启动线程池回声服务器
./v4_epoll_thread_pool        # 启动 epoll + 线程池回声服务器
```

另开终端用 `nc` 测试（聊天室可开多个窗口互发）：

```bash
nc 127.0.0.1 8888
```

## 目录结构

```
linux-tcp-server/
├── README.md
├── Makefile
├── v1_echo_server.c        # 阻塞单连接回声服务器
├── v2_chat_server.c        # 多线程聊天室服务器
├── v3_thread_pool_server.c # 线程池回声服务器
└── v4_epoll_thread_pool.c  # epoll + 线程池回声服务器
```

## 面试常考知识点（本项目已覆盖 / 将覆盖）

- TCP 与 socket 编程完整流程
- 阻塞 IO 模型 vs 多线程并发模型 vs 线程池模型
- 线程安全：互斥锁保护共享资源、避免竞态条件
- 线程间参数传递的坑（`malloc` 拷值 vs 栈变量）
- 条件变量与互斥锁协作（`cond_wait` 用 `while` 防虚假唤醒、避免持锁做 IO）
- `SIGPIPE` 信号与网络服务器健壮性
- 循环 `recv` / `send` 处理短读短写（partial read / write）
- epoll 多路复用、LT / ET 模式、epoll + 线程池（`EPOLLONESHOT` 防竞态、重新 ARM 的必要性）

## 环境要求

- Linux（推荐 WSL2 Ubuntu 或原生 Linux）
- gcc、pthread 库（`-pthread`）
