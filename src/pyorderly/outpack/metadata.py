from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union

from dataclasses_json import DataClassJsonMixin

from pyorderly.outpack.hash import hash_file
from pyorderly.outpack.tools import GitInfo


@dataclass
class PacketFile(DataClassJsonMixin):
    path: str
    size: int
    hash: str

    @staticmethod
    def from_file(directory, path, hash_algorithm):
        f = Path(directory).joinpath(path)
        s = f.stat().st_size
        h = str(hash_file(f, hash_algorithm))
        return PacketFile(path, s, h)


@dataclass
class PacketFileWithLocation(PacketFile):
    location: str
    packet_id: str

    @staticmethod
    def from_packet_file(file: PacketFile, location: str, packet_id: str):
        return PacketFileWithLocation(
            file.path, file.size, file.hash, location, packet_id
        )


@dataclass
class PacketDependsPath(DataClassJsonMixin):
    here: str
    there: str


@dataclass
class PacketDepends(DataClassJsonMixin):
    packet: str
    query: str
    files: list[PacketDependsPath]

    @staticmethod
    def files_from_dict(files):
        return [{"here": h, "there": t} for h, t, in files.items()]


Parameters = dict[str, Union[bool, int, float, str]]


@dataclass
class MetadataCore(DataClassJsonMixin):
    schema_version: str
    id: str
    name: str
    parameters: Parameters
    time: dict[str, float]
    files: list[PacketFile]
    depends: list[PacketDepends]
    git: Optional[GitInfo]
    custom: Optional[dict]

    def file_hash(self, name):
        for x in self.files:
            if x.path == name:
                return x.hash
        msg = f"Packet {self.id} does not contain file '{name}'"
        raise Exception(msg)


@dataclass
class PacketLocation(DataClassJsonMixin):
    packet: str
    time: float
    hash: str


def read_metadata_core(path) -> MetadataCore:
    with open(path) as f:
        return MetadataCore.from_json(f.read().strip())


def read_packet_location(path) -> PacketLocation:
    with open(path) as f:
        return PacketLocation.from_json(f.read().strip())
