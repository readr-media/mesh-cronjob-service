from fastapi_cache import FastAPICache

async def get_cache(backend, cache_key: str):
    try:
        backend  = FastAPICache.get_backend()
        ttl, cached = await backend.get_with_ttl(cache_key)
    except Exception:
        print(f"Error retrieving cache key '{cache_key}' from backend")
        ttl, cached = 0, None
    return ttl, cached

async def set_cache(cache_key: str, cache_value: str, ttl: int):
    try:
        backend  = FastAPICache.get_backend()
        await backend.set(cache_key, cache_value, expire=ttl)
    except Exception as e:
        print(f"Error setting cache key '{cache_key}' from backend, error: {e}")