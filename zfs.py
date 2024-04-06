from __future__ import annotations
from datetime import datetime
import subprocess
from typing import Optional
from dataclasses import dataclass


@dataclass(eq=True, frozen=True)
class Snapshot:
  name: str
  timestamp: datetime

@dataclass(eq=True, frozen=True)
class Filesystem:
  name: str


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
      timestamp = datetime.fromtimestamp(int(fields[1]))
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
