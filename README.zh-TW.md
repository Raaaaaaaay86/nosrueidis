<h1 align="center"> nosrueidis </h1>

<p>
nosrueidis 將 <a href="https://github.com/redis/rueidis">rueidis</a> 與 <a href="https://github.com/redis/rueidis/tree/main/rueidislock">rueidislock</a> 整合進單一的 RueidisClient struct。提供帶有逾時限制的 Session 輔助函式、具表達力的 Execute / ExecuteCacheable 方法（支援 client-side cache 與 debug logging），以及分散式鎖的 DistributedLocker 介面。
</p>

<p align="center">
  <a href="README.md">English</a>
</p>

## 安裝

```bash
go get github.com/raaaaaaaay86/nosrueidis
```

## 快速開始

### 建立 Client

```go
package main

import (
    "time"

    "github.com/raaaaaaaay86/nosrueidis"
)

func main() {
    client, err := nosrueidis.NewRueidisClient(nosrueidis.RueidisConfig{
        Endpoints:        []string{"localhost:6379"},
        User:             "",
        Password:         "",
        SelectDB:         0,
        ExecutionTimeout: 3 * time.Second,
    })
    if err != nil {
        panic(err)
    }
}
```

### 實際範例（noschat）

```go
// component/redis.go

func NewRueidisClient(cfg *config.Application) (*nosrueidis.RueidisClient, error) {
    rcfg := cfg.Redis.App
    return nosrueidis.NewRueidisClient(nosrueidis.RueidisConfig{
        Endpoints:        rcfg.Hosts,
        User:             rcfg.Username,
        Password:         rcfg.Password,
        SelectDB:         rcfg.SelectDB,
        ExecutionTimeout: rcfg.GetExecutionTimeout(),
    })
}
```

## 執行指令

使用 `client.GetClient()` 取得底層的 `rueidis.Client` 來建構指令，再將完成的指令傳入 `Execute` 或 `ExecuteCacheable`。

```go
type GetUserCmd struct {
    c   rueidis.Client
    key string
}

func (cmd GetUserCmd) Build() rueidis.Completed {
    return cmd.c.B().Get().Key(cmd.key).Build()
}

result, err := client.Execute(ctx, GetUserCmd{c: client.GetClient(), key: "user:42"})
if err != nil {
    return err
}

userJson, err := result.ToString()
```

## Client-Side Cache

傳入 `WithClientSideTtl` 啟用 rueidis 內建的 client-side cache。在 TTL 期間內的後續讀取將從本地記憶體提供，無需網路往返。

```go
type GetUserCacheCmd struct {
    c   rueidis.Client
    key string
}

func (cmd GetUserCacheCmd) Build() rueidis.Completed {
    return cmd.c.B().Get().Key(cmd.key).Build()
}

func (cmd GetUserCacheCmd) Cache() rueidis.Cacheable {
    return cmd.c.B().Get().Key(cmd.key).Cache()
}

result, err := client.ExecuteCacheable(ctx, GetUserCacheCmd{c: client.GetClient(), key: "user:42"},
    nosrueidis.WithClientSideTtl(30 * time.Second),
)
```

## Debug 日誌

```go
result, err := client.Execute(ctx, myCmd,
    nosrueidis.WithDebugLogging("user:42"),
)
// 輸出：redis command result key=user:42 client_side=true/false
```

## Session 輔助函式

`Session` 回傳一個已設定 `ExecutionTimeout` 的 context、原始的 `rueidis.Client` 以及 cancel 函式。當你需要更精細的控制或需要串接多個指令時使用。

```go
sessCtx, cancel, c := client.Session(ctx)
defer cancel()

result := c.Do(sessCtx, c.B().Set().Key("key").Value("value").Ex(60).Build())
if err := result.Error(); err != nil {
    return err
}
```

## 分散式鎖

```go
locker := client.GetLock()

ctx, release, err := locker.WithContext(ctx, "my-resource-lock")
if err != nil {
    return err // 未能取得鎖
}
defer release()

// 臨界區段
```

## API 說明

### `RueidisConfig`

| 欄位               | 型別            | 說明                           |
|--------------------|-----------------|--------------------------------|
| `Endpoints`        | `[]string`      | Redis 節點位址清單             |
| `User`             | `string`        | ACL 使用者名稱（無驗證時留空） |
| `Password`         | `string`        | 密碼                           |
| `SelectDB`         | `int`           | 資料庫索引                     |
| `ExecutionTimeout` | `time.Duration` | 每次指令的執行逾時             |

### `RueidisClient` 方法

| 方法                                  | 說明                                                       |
|---------------------------------------|------------------------------------------------------------|
| `Execute(ctx, cmd, opts...)`          | 執行不可快取的指令                                         |
| `ExecuteCacheable(ctx, cmd, opts...)` | 執行可快取的指令（可選擇性開啟 client-side TTL）           |
| `Session(ctx)`                        | 回傳帶逾時的 `ctx, cancel, rueidis.Client`                 |
| `GetClient()`                         | 取得原始 `rueidis.Client`                                  |
| `GetLock()`                           | 取得 `rueidislock.Locker`                                  |

### 執行選項

| 選項                          | 說明                                        |
|-------------------------------|---------------------------------------------|
| `WithClientSideTtl(duration)` | 以指定 TTL 啟用 client-side cache           |
| `WithDebugLogging(key)`       | 以 `slog.Debug` 層級記錄 cache hit/miss     |
