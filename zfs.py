from __future__ import annotations
from datetime import datetime
import subprocess
from typing import Optional


class Snapshot:
  name: str
  timestamp: datetime

  def __init__(self, name: str, creation: int):
    self.name = name
    self.timestamp = datetime.fromtimestamp(creation)
      
  def __repr__(self):
    return f'Snapshot(name="{self.name}", timestamp={self.timestamp.strftime("%Y-%m-%d %H:%M:%S")})'


class Filesystem:
  name: str

  def __init__(self, name: str) -> None:
    self.name = name

  def __repr__(self):
    return f'Filesystem(name="{self.name}")'


def run_zfs_command(cmd: list[str]) -> str:
  r = subprocess.run(['zfs', *cmd], capture_output=True, text=True)
  try:
    r.check_returncode()
  except subprocess.CalledProcessError:
    print(f'ERROR: {r.stderr.strip()}')
    raise
  return r.stdout.strip()


def get_snapshots(dataset: Optional[str] = None) -> set[Snapshot]:
  cmd = ['list', '-H', '-t', 'snapshot', '-p', '-o', 'name,creation']
  if dataset:
    cmd.append(dataset)
  lines = run_zfs_command(cmd).splitlines()
  snapshots: set[Snapshot] = set()

  for line in lines:
    fields = line.split('\t')
    snapshots.add(Snapshot(
      name = fields[0],
      creation = int(fields[1])
    ))

  return snapshots

def destroy_dataset(dataset: str) -> None:
  run_zfs_command(['destroy', dataset])

def get_filesystems() -> set[Filesystem]:
  lines = run_zfs_command(['list', '-H', '-t', 'filesystem']).splitlines()
  filesystems: set[Filesystem] = set()

  for line in lines:
    fields = line.split('\t')
    filesystems.add(Filesystem(
      name = fields[0]
    ))

  return filesystems
