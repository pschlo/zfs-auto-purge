from __future__ import annotations
from datetime import datetime
import subprocess
from subprocess import Popen, PIPE
from typing import Optional, IO
from collections.abc import Collection
from dataclasses import dataclass


@dataclass(eq=True, frozen=True)
class Snapshot:
  dataset: str
  short_name: str
  timestamp: datetime
  guid: int

  @property
  def full_name(self):
    return f'{self.dataset}@{self.short_name}'
  

class ZfsCli:
  base_cmd: list[str]

  def __init__(self, base_cmd: list[str]) -> None:
    self.base_cmd = base_cmd
  

  # def run_ssh_command(cmd: list[str], host: str, user: Optional[str], port: Optional[str]) -> str:
  #   r = subprocess.run(['ssh', *cmd], capture_output=True)


  def _run_command(self, cmd: list[str]) -> str:
    r = subprocess.run([*self.base_cmd, *cmd], capture_output=True, text=True)
    try:
      r.check_returncode()
    except subprocess.CalledProcessError:
      print(f'ERROR: {r.stderr.strip()}')
      raise
    return r.stdout.strip()
  
  def send_snapshot_async(self, snapshot: Snapshot, base: Optional[Snapshot] = None) -> Popen[bytes]:
    cmd = [*self.base_cmd, 'send']
    if base is not None:
      cmd += ['-I', base.full_name]
    cmd += [snapshot.full_name]
    return Popen(cmd, stdout=PIPE)
  
  def receive_snapshot_async(self, dataset: str, stdin: IO[bytes]) -> Popen[bytes]:
    cmd = [*self.base_cmd, 'receive', dataset]
    return Popen(cmd, stdin=stdin)
  

  def rename_snapshot(self, snapshot: Snapshot, new_short_name: str) -> Snapshot:
    self._run_command(['rename', snapshot.full_name, f'@{new_short_name}'])
    return Snapshot(
      dataset=snapshot.dataset,
      short_name=new_short_name,
      timestamp=snapshot.timestamp,
      guid=snapshot.guid
    )


  def create_snapshot(self, dataset: str, short_name: str, recursive: bool = False) -> Snapshot:
    full_name = f'{dataset}@{short_name}'
    
    # take snapshot
    cmd = ['snapshot']
    if recursive:
      cmd += ['-r']
    cmd += [full_name]
    self._run_command(cmd)

    # fetch infos and return Snapshot object
    cmd = ['get', '-Hp', '-o', 'value', 'creation,guid', full_name]
    timestamp, guid = self._run_command(cmd).splitlines()
    return Snapshot(
      dataset=dataset,
      short_name=short_name,
      timestamp=datetime.fromtimestamp(int(timestamp)),
      guid=int(guid)
    )


  def get_snapshots(self, dataset: Optional[str] = None, recursive: bool = False) -> set[Snapshot]:
    cmd = ['list', '-Hp', '-t', 'snapshot', '-o', 'name,creation,guid']
    if recursive:
      cmd += ['-r']
    if dataset:
      cmd += [dataset]
    lines = self._run_command(cmd).splitlines()
    snapshots: set[Snapshot] = set()

    for line in lines:
      fields = line.split('\t')
      _dataset, _short_name = fields[0].split('@')
      snap = Snapshot(
        dataset = _dataset,
        short_name = _short_name,
        timestamp = datetime.fromtimestamp(int(fields[1])),
        guid = int(fields[2])
      )
      snapshots.add(snap)

    return snapshots


  def destroy_snapshots(self, snapshots: Collection[Snapshot]) -> None:
    # group snapshots by dataset
    _map: dict[str, set[Snapshot]] = dict()
    for snap in snapshots:
      if snap.dataset not in _map:
        _map[snap.dataset] = set()
      _map[snap.dataset].add(snap)

    # run one command per dataset
    for dataset, snaps in _map.items():
      short_names = ','.join(map(lambda s: s.short_name, snaps))
      self._run_command(['destroy', f'{dataset}@{short_names}'])



class LocalZfsCli(ZfsCli):
  def __init__(self) -> None:
    super().__init__(['zfs'])


class RemoteZfsCli(ZfsCli):
  def __init__(self, host: str, user: Optional[str], port: Optional[int]) -> None:
    cmd = ['ssh']
    if user is not None:
      cmd += ['-l', user]
    if port is not None:
      cmd += ['-p', str(port)]
    cmd += [host, 'zfs']

    super().__init__(cmd)
