import os
import shutil
from pathlib import Path
from typing import Dict, List

from typing_extensions import override

from pyorderly.outpack.location_driver import LocationDriver
from pyorderly.outpack.metadata import MetadataCore, PacketFile, PacketLocation
from pyorderly.outpack.root import find_file_by_hash, root_open
from pyorderly.outpack.static import LOCATION_LOCAL
from pyorderly.outpack.util import read_string


class OutpackLocationPath(LocationDriver):
    def __init__(self, path):
        self.__root = root_open(path, locate=False)

    @override
    def __enter__(self):
        return self

    @override
    def __exit__(self, exc_type, exc_value, exc_tb):
        pass

    @override
    def list(self) -> Dict[str, PacketLocation]:
        return self.__root.index.location(LOCATION_LOCAL)

    @override
    def metadata(self, packet_ids: List[str]) -> Dict[str, str]:
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
    def fetch_file(self, _packet: MetadataCore, file: PacketFile, dest: str):
        if self.__root.config.core.use_file_store:
            path = self.__root.files.filename(file.hash)
            if not os.path.exists(path):
                msg = f"Hash '{file.hash}' not found at location"
                raise Exception(msg)
        else:
            path = find_file_by_hash(self.__root, file.hash)
            if path is None:
                msg = f"Hash '{file.hash}' not found at location"
                raise Exception(msg)
        shutil.copyfile(path, dest)

    @override
    def list_unknown_packets(self, ids: List[str]) -> List[str]:
        raise NotImplementedError()

    @override
    def list_unknown_files(self, hashes: List[str]) -> List[str]:
        raise NotImplementedError()

    @override
    def push_file(self, src: Path, hash: str):
        raise NotImplementedError()

    @override
    def push_metadata(self, src: Path, hash: str):
        raise NotImplementedError()
