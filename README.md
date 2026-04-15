# nosrueidis

`nosrueidis` wraps [rueidis](https://github.com/redis/rueidis) and [rueidislock](https://github.com/redis/rueidis/tree/main/rueidislock) into a single `RueidisClient` struct. It provides a `Session` helper for timeout-bounded execution, expressive `Execute` / `ExecuteCacheable` methods with optional client-side caching and debug logging, and a `DistributedLocker` interface for distributed locks.

## Installation

```bash
go get github.com/raaaaaaaay86/nosrueidis
```

## Quick Start

### Create a Client

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

### Real-World Example (noschat)

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

## Execute Commands

Use `client.GetClient()` to access the underlying `rueidis.Client` for building commands, then pass the completed command to `Execute` or `ExecuteCacheable`.

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

Pass `WithClientSideTtl` to enable rueidis's built-in client-side cache. Subsequent reads within the TTL are served from local memory without a network round-trip.

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

## Debug Logging

```go
result, err := client.Execute(ctx, myCmd,
    nosrueidis.WithDebugLogging("user:42"),
)
// Logs: redis command result key=user:42 client_side=true/false
```

## Session Helper

`Session` returns a context pre-set with `ExecutionTimeout`, the raw `rueidis.Client`, and a cancel function. Use this when you need finer-grained control or need to chain multiple commands.

```go
sessCtx, cancel, c := client.Session(ctx)
defer cancel()

result := c.Do(sessCtx, c.B().Set().Key("key").Value("value").Ex(60).Build())
if err := result.Error(); err != nil {
    return err
}
```

## Distributed Lock

```go
locker := client.GetLock()

ctx, release, err := locker.WithContext(ctx, "my-resource-lock")
if err != nil {
    return err // lock not acquired
}
defer release()

// critical section
```

## API Reference

### `RueidisConfig`

| Field              | Type            | Description                           |
|--------------------|-----------------|---------------------------------------|
| `Endpoints`        | `[]string`      | Redis node addresses                  |
| `User`             | `string`        | ACL username (empty for no auth)      |
| `Password`         | `string`        | Password                              |
| `SelectDB`         | `int`           | Database index                        |
| `ExecutionTimeout` | `time.Duration` | Per-command execution timeout         |

### `RueidisClient` Methods

| Method                                | Description                                                   |
|---------------------------------------|---------------------------------------------------------------|
| `Execute(ctx, cmd, opts...)`          | Execute a non-cacheable command                               |
| `ExecuteCacheable(ctx, cmd, opts...)` | Execute a cacheable command (optionally with client-side TTL) |
| `Session(ctx)`                        | Return `ctx, cancel, rueidis.Client` with timeout             |
| `GetClient()`                         | Return the raw `rueidis.Client`                               |
| `GetLock()`                           | Return the `rueidislock.Locker`                               |

### Execution Options

| Option                        | Description                                     |
|-------------------------------|-------------------------------------------------|
| `WithClientSideTtl(duration)` | Enable client-side cache with given TTL         |
| `WithDebugLogging(key)`       | Log cache hit/miss at `slog.Debug` level        |
