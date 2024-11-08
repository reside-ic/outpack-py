from abc import abstractmethod
from contextlib import AbstractContextManager
from pathlib import Path

from typing_extensions import Self

from pyorderly.outpack.metadata import MetadataCore, PacketFile, PacketLocation


class LocationDriver(AbstractContextManager):
    """
    A location implementation.

    The driver object is treated as a context manager and is entered and exited
    before and after its methods are called.
    """

    # This is already defined in AbstractContextManager, but Python/mypy don't
    # handle F-bounded polymorphism all that well, which prevents us from
    # giving AbstractContextManager a useful type-parameter.
    @abstractmethod
    def __enter__(self) -> Self: ...

    @abstractmethod
    def list_packets(self) -> dict[str, PacketLocation]: ...

    @abstractmethod
    def metadata(self, packet_ids: list[str]) -> dict[str, str]: ...

    @abstractmethod
    def fetch_file(
        self, packet: MetadataCore, file: PacketFile, dest: str
    ) -> None: ...

    @abstractmethod
    def list_unknown_packets(self, ids: list[str]) -> list[str]: ...

    @abstractmethod
    def list_unknown_files(self, hashes: list[str]) -> list[str]: ...

    @abstractmethod
    def push_file(self, src: Path, hash: str): ...

    @abstractmethod
    def push_metadata(self, src: Path, hash: str): ...
