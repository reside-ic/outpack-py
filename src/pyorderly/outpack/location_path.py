import shutil
from pathlib import Path

from typing_extensions import Self, override

from pyorderly.outpack.location_driver import LocationDriver
from pyorderly.outpack.metadata import MetadataCore, PacketFile, PacketLocation
from pyorderly.outpack.root import root_open
from pyorderly.outpack.static import LOCATION_LOCAL
from pyorderly.outpack.util import read_string


class OutpackLocationPath(LocationDriver):
    def __init__(self, path: str):
        self.__root = root_open(path, locate=False)

    @override
    def __enter__(self) -> Self:
        return self

    @override
    def __exit__(self, exc_type, exc_value, exc_tb):
        pass

    @override
    def list_packets(self) -> dict[str, PacketLocation]:
        return self.__root.index.location(LOCATION_LOCAL)

    @override
    def metadata(self, packet_ids: list[str]) -> dict[str, str]:
        all_ids = self.__root.index.location(LOCATION_LOCAL).keys()
        missing_ids = set(packet_ids).difference(all_ids)
        if missing_ids:
            missing_msg = "', '".join(missing_ids)
            msg = f"Some packet ids not found: '{missing_msg}'"
            raise Exception(msg)
        ret = {}
        for packet_id in packet_ids:
            path = self.__root.path / ".outpack" / "metadata" / packet_id
            ret[packet_id] = read_string(path)
        return ret

    @override
    def fetch_file(self, packet: MetadataCore, file: PacketFile, dest: str):
        try:
            src = self.__root.find_file_by_hash(
                file.hash, candidates=[packet.id]
            )
        except FileNotFoundError as e:
            msg = f"Hash '{file.hash}' not found at location"
            raise Exception(msg) from e

        shutil.copyfile(src, dest)

    @override
    def list_unknown_packets(self, ids: list[str]) -> list[str]:
        raise NotImplementedError()

    @override
    def list_unknown_files(self, hashes: list[str]) -> list[str]:
        raise NotImplementedError()

    @override
    def push_file(self, src: Path, hash: str):
        raise NotImplementedError()

    @override
    def push_metadata(self, src: Path, hash: str):
        raise NotImplementedError()
