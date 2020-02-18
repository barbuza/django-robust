from typing import Iterable, Optional


class RedisApi:
    def llen(self, key: str) -> int:
        ...

    def lpush(self, key: str, item: bytes) -> None:
        ...

    def ltrim(self, key: str, from_index: int, to_index: int) -> None:
        ...

    def lindex(self, key: str, index: int) -> Optional[bytes]:
        ...

    def flushall(self) -> None:
        ...

    def keys(self, pattern: Optional[str]) -> Iterable[str]:
        ...


class Pipeline(RedisApi):
    def execute(self) -> None:
        pass


class Redis(RedisApi):
    def pipeline(self, transaction: Optional[bool]) -> Pipeline:
        ...
