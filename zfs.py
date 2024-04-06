from __future__ import annotations
from datetime import datetime
import subprocess
from typing import Optional
from dataclasses import dataclass
from re import Pattern


@dataclass(eq=True, frozen=True)
class Snapshot:
  dataset: str
  short_name: str
  timestamp: datetime

  @property
  def full_name(self):
    return f'{self.dataset}@{self.short_name}'


def run_zfs_command(cmd: list[str]) -> str:
  r = subprocess.run(['zfs', *cmd], capture_output=True, text=True)
  try:
    r.check_returncode()
  except subprocess.CalledProcessError:
    print(f'ERROR: {r.stderr.strip()}')
    raise
  return r.stdout.strip()


def get_snapshots(dataset: Optional[str] = None, recursive: bool = False, match_name: Optional[Pattern] = None) -> set[Snapshot]:
  cmd = ['list', '-H', '-t', 'snapshot', '-p', '-o', 'name,creation']
  if recursive:
    cmd.append('-r')
  if dataset:
    cmd.append(dataset)
  lines = run_zfs_command(cmd).splitlines()
  snapshots: set[Snapshot] = set()

  for line in lines:
    fields = line.split('\t')

    _dataset, _short_name = fields[0].split('@')
    snap = Snapshot(
      dataset = _dataset,
      short_name = _short_name,
      timestamp = datetime.fromtimestamp(int(fields[1]))
    )

    if match_name is None or match_name.match(snap.short_name):
      snapshots.add(snap)

  return snapshots


def destroy_snapshot(snapshot: Snapshot) -> None:
  run_zfs_command(['destroy', snapshot.full_name])
