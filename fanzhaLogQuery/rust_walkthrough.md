# Rust 日志查询工具 - 构建与运行指南

高性能 Rust 版本的日志查询工具已生成在 `../RustProject/fanzhaLogQuery` 目录中。

## 前置要求

您必须安装 Rust。如果尚未安装，请通过以下命令安装：
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

## 构建说明

1. 进入项目目录：
   ```bash
   cd ../RustProject/fanzhaLogQuery
   ```

2. 以 release 模式构建项目（对性能至关重要）：
   ```bash
   cargo build --release
   ```

   可执行文件将位于 `target/release/fanzha_log_query`。

## 运行

1. 将您的 `config.yaml` 复制到 Rust 项目目录或运行二进制文件的目录。
2. 运行工具：
   ```bash
   ./target/release/fanzha_log_query
   ```

## 交叉编译与部署（用于 Linux 服务器）

由于您使用的是 macOS (ARM64)，而服务器很可能是 Linux (x86_64)，因此不能直接复制本地二进制文件。您有两个选择：

### 方案 A: 在服务器上编译（推荐新手使用）
如果您的服务器有互联网访问，这是最简单的方法。

1.  **在服务器上安装 Rust**：
    ```bash
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
    ```
2.  **上传源代码**：将整个 `fanzhaLogQuery` 文件夹复制到服务器。
3.  **构建**：
    ```bash
    cd fanzhaLogQuery
    cargo build --release
    ```
4.  **运行**：
    ```bash
    ./target/release/fanzha_log_query
    ```

### 方案 B: 从 macOS 交叉编译
如果您必须在 Mac 上编译，请使用 `cross` 来构建静态 Linux 二进制文件。

1.  **安装 Docker**：`cross` 需要 Docker Desktop 运行。
2.  **安装 cross**：
    ```bash
    cargo install cross
    ```
3.  **为 Linux 构建**：
    ```bash
    cross build --target x86_64-unknown-linux-musl --release
    ```
    *注意：`musl` 创建的静态二进制文件可以在任何 Linux 发行版上运行，无需依赖。*

4.  **定位二进制文件**：
    二进制文件将位于 `target/x86_64-unknown-linux-musl/release/fanzha_log_query`。

## 部署检查清单

迁移到服务器时，请确保在同一目录中有以下两个文件：

1.  **二进制文件**：`fanzha_log_query`（确保执行 `chmod +x fanzha_log_query`）
2.  **配置文件**：`config.yaml`（编辑 `logDirectory` 以匹配服务器的路径！）

## 性能说明

- **SIMD 加速**：代码使用 `memchr` 进行超快速分隔符查找。
- **并行处理**：`rayon` 自动扩展以使用所有可用的 CPU 核心。
- **快速解压**：使用 `miniz_oxide`（纯 Rust），比标准 Go gzip 快得多，且不需要像 `cmake` 这样的外部 C 依赖。
