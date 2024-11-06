from abc import abstractmethod
from contextlib import AbstractContextManager
from pathlib import Path
from typing import Dict, List

from pyorderly.outpack.metadata import MetadataCore, PacketFile, PacketLocation


class LocationDriver(AbstractContextManager):
    """
    A location implementation.

    The driver object is treated as a context manager and is entered and exited
    before and after its methods are called.
    """

    @abstractmethod
    def list(self) -> Dict[str, PacketLocation]: ...

    @abstractmethod
    def metadata(self, packet_ids: List[str]) -> Dict[str, str]: ...

    @abstractmethod
    def fetch_file(
        self, packet: MetadataCore, file: PacketFile, dest: str
    ) -> None: ...

    @abstractmethod
    def list_unknown_packets(self, ids: List[str]) -> List[str]: ...

    @abstractmethod
    def list_unknown_files(self, hashes: List[str]) -> List[str]: ...

    @abstractmethod
    def push_file(self, src: Path, hash: str): ...

    @abstractmethod
    def push_metadata(self, src: Path, hash: str): ...
