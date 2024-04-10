from __future__ import annotations
from datetime import datetime
import subprocess
from subprocess import Popen, PIPE, CompletedProcess, CalledProcessError
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
  
  def with_dataset(self, dataset: str) -> Snapshot:
    return Snapshot(
      dataset=dataset,
      short_name=self.short_name,
      timestamp=self.timestamp,
      guid=self.guid
    )


@dataclass(eq=True, frozen=True)
class Pool:
  name: str
  guid: int


class ZfsCli:
  def run_text_command(self, cmd: list[str]) -> str:
    p: Popen[str] = self.start_command(cmd, stdout=PIPE, text=True)
    stdout, _ = p.communicate()
    if p.returncode > 0:
      raise CalledProcessError(p.returncode, cmd=p.args, output=stdout)
    return stdout.strip()
  
  def start_command(self, cmd: list[str], stdin=None, stdout=None, stderr=None, text=False) -> Popen:
    return Popen(cmd, stdin=stdin, stdout=stdout, stderr=stderr, text=text)
  
  def send_snapshot_async(self, snapshot: Snapshot, base: Optional[Snapshot] = None) -> Popen[bytes]:
    cmd = ['zfs', 'send']
    if base is not None:
      cmd += ['-i', base.full_name]
    cmd += [snapshot.full_name]
    return self.start_command(cmd, stdout=PIPE)
  
  def receive_snapshot_async(self, dataset: str, stdin: IO[bytes]) -> Popen[bytes]:
    cmd = ['zfs', 'receive', dataset]
    return self.start_command(cmd, stdin=stdin)
  

  def rename_snapshot(self, snapshot: Snapshot, new_short_name: str) -> Snapshot:
    self.run_text_command(['zfs', 'rename', snapshot.full_name, f'@{new_short_name}'])
    return Snapshot(
      dataset=snapshot.dataset,
      short_name=new_short_name,
      timestamp=snapshot.timestamp,
      guid=snapshot.guid
    )

  # TrueNAS CORE 13.0 does not support holds -p, so we do not fetch timestamp
  def get_hold_tags(self, snapshots: Collection[Snapshot]) -> set[str]:
    lines = self.run_text_command(['zfs', 'holds', '-H', ' '.join(s.full_name for s in snapshots)]).splitlines()
    tags: set[str] = {line.split('\t')[1] for line in lines}
    return tags
  
  def hold(self, snapshots: Collection[Snapshot], tag: str) -> None:
    self.run_text_command(['zfs', 'hold', tag, ' '.join(s.full_name for s in snapshots)])

  def release(self, snapshots: Collection[Snapshot], tag: str) -> None:
    self.run_text_command(['zfs', 'release', tag, ' '.join(s.full_name for s in snapshots)])

  def get_pool_from_dataset(self, dataset: str) -> Pool:
    name = dataset.split('/')[0]
    guid = self.run_text_command(['zpool', 'get', '-Hp', '-o', 'value', 'guid', name])
    return Pool(name=name, guid=int(guid))

  def create_snapshot(self, dataset: str, short_name: str, recursive: bool = False) -> None:
    full_name = f'{dataset}@{short_name}'
    
    # take snapshot
    cmd = ['zfs', 'snapshot']
    if recursive:
      cmd += ['-r']
    cmd += [full_name]
    self.run_text_command(cmd)

  def get_snapshots(self, dataset: Optional[str] = None, recursive: bool = False) -> set[Snapshot]:
    cmd = ['zfs', 'list', '-Hp', '-t', 'snapshot', '-o', 'name,creation,guid']
    if recursive:
      cmd += ['-r']
    if dataset:
      cmd += [dataset]
    lines = self.run_text_command(cmd).splitlines()
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
      try:
        self.run_text_command(['zfs', 'destroy', f'{dataset}@{short_names}'])
      except CalledProcessError as e:
        # ignore if destroy failed with code 1
        if e.returncode == 1: pass
        raise



class LocalZfsCli(ZfsCli):
  pass


class RemoteZfsCli(ZfsCli):
  ssh_command: list[str]

  def __init__(self, host: str, user: Optional[str], port: Optional[int]) -> None:
    super().__init__()

    cmd = ['ssh']
    if user is not None:
      cmd += ['-l', user]
    if port is not None:
      cmd += ['-p', str(port)]
    cmd += [host]
    self.ssh_command = cmd

  def start_command(self, cmd: list[str], stdin=None, stdout=None, stderr=None, text=False) -> Popen:
    cmd = self.ssh_command + cmd
    return super().start_command(cmd, stdin, stdout, stderr, text)
