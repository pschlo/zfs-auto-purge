from __future__ import annotations
from datetime import datetime
import subprocess


class Snapshot:
    name: str
    timestamp: datetime

    def __init__(self, name: str, creation: int):
        self.name = name
        self.timestamp = datetime.fromtimestamp(creation)
        
    def __repr__(self):
        return f'Snapshot(name={self.name}, timestamp={self.timestamp.strftime("%Y-%m-%d %H:%M:%S")})'


class Filesystem:
  name: str

  def __init__(self, name: str) -> None:
    name = name


def run_zfs_command(cmd: list[str]) -> str:
  return subprocess.run(['zfs', *cmd], capture_output=True, text=True, check=True).stdout


def get_snapshots() -> set[Snapshot]:
  lines = run_zfs_command(['list', '-H', '-t', 'snapshot', '-p', '-o', 'name,creation']).splitlines()
  snapshots: set[Snapshot] = set()

  for line in lines:
    fields = line.split('\t')
    snapshots.add(Snapshot(
      name = fields[0],
      creation = int(fields[1])
    ))

  return snapshots


def get_filesystems() -> set[Filesystem]:
  lines = run_zfs_command(['list', '-H', '-t', 'filesystem']).splitlines()
  filesystems: set[Filesystem] = set()

  for line in lines:
    fields = line.split('\t')
    filesystems.add(Filesystem(
      name = fields[0]
    ))

  return filesystems
