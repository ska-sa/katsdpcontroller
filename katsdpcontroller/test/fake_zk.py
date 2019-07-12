"""Fake implementation of aiozk for testing

It only implements a small subset of the aiozk interface, and several
functions don't return values that they should.
"""

from typing import Dict, Tuple

import aiozk


class _Node:
    def __init__(self, content: bytes = b'', version: int = 1) -> None:
        self.content = content
        self.version = version


class ZKClient:
    def __init__(self, server: str, chroot: str = None) -> None:
        self._nodes: Dict[str, _Node] = {'/': _Node()}

    def normalize_path(self, path: str) -> str:
        return '/' + '/'.join(name for name in path.split('/') if name)

    def _parent(self, path: str) -> str:
        return self.normalize_path(path.rsplit('/', 1)[0])

    async def start(self) -> None:
        pass

    async def close(self) -> None:
        pass

    async def create(self, path: str, data: bytes = None, ephemeral=False) -> None:
        path = self.normalize_path(path)
        if path in self._nodes:
            raise aiozk.exc.NodeExists
        if self._parent(path) not in self._nodes:
            raise aiozk.exc.NoNode
        content = data if data is not None else b''
        self._nodes[path] = _Node(content)

    async def set(self, path: str, data: bytes, version: int) -> None:
        path = self.normalize_path(path)
        node = self._nodes.get(path)
        if node is None:
            raise aiozk.exc.NoNode
        if version >= 0 and node.version != version:
            raise aiozk.exc.BadVersion
        node.content = data
        node.version += 1

    async def set_data(self, path: str, data: bytes, force: bool = False) -> None:
        await self.set(path, data, -1)

    async def get(self, path: str) -> Tuple[bytes, None]:
        path = self.normalize_path(path)
        node = self._nodes.get(path)
        if node is None:
            raise aiozk.exc.NoNode
        return node.content, None

    async def ensure_path(self, path: str) -> None:
        path = self.normalize_path(path)
        if path != '/':
            await self.ensure_path(self._parent(path))
        if path not in self._nodes:
            self._nodes[path] = _Node()