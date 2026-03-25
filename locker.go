package nosrueidis

import "context"

type ReleaseFn func()

type DistributedLocker interface {
	// Lock 取得分散鎖
	Lock(ctx context.Context, key string) (ReleaseFn, error)
	// TryLock 使用TryLock邏輯取得分散鎖
	TryLock(ctx context.Context, key string) (ReleaseFn, error)
}
