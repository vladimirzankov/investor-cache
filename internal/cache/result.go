package cache

type CacheResult string

const (
	CacheResultHit     CacheResult = "hit"
	CacheResultMiss    CacheResult = "miss"
	CacheResultBypass  CacheResult = "bypass"
	CacheResultError   CacheResult = "error"
	CacheResultCBOpen  CacheResult = "cb_open"
	CacheResultUnknown CacheResult = "unknown"
)
