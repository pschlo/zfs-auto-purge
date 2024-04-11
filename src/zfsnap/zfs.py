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
  shortname: str
  timestamp: datetime
  guid: int

  @property
  def fullname(self):
    return f'{self.dataset}@{self.shortname}'
  
  def with_dataset(self, dataset: str) -> Snapshot:
    return Snapshot(
      dataset=dataset,
      shortname=self.shortname,
      timestamp=self.timestamp,
      guid=self.guid
    )


@dataclass(eq=True, frozen=True)
class Pool:
  name: str
  guid: int

@dataclass(eq=True, frozen=True)
class Hold:
  snapshot_fullname: str
  tag: str


"""
Each method call should correspond to exactly one CLI call
"""
class ZfsCli:
  def run_text_command(self, cmd: list[str]) -> str:
    p: Popen[str] = self.start_command(cmd, stdout=PIPE, text=True)
    stdout, _ = p.communicate()
    if p.returncode > 0:
      raise CalledProcessError(p.returncode, cmd=p.args, output=stdout)
    return stdout.strip()
  
  def start_command(self, cmd: list[str], stdin=None, stdout=None, stderr=None, text=False) -> Popen:
    return Popen(cmd, stdin=stdin, stdout=stdout, stderr=stderr, text=text)
  
  def send_snapshot_async(self, snapshot_fullname: str, base_fullname: Optional[str] = None) -> Popen[bytes]:
    cmd = ['zfs', 'send']
    if base_fullname is not None:
      cmd += ['-i', base_fullname]
    cmd += [snapshot_fullname]
    return self.start_command(cmd, stdout=PIPE)
  
  def receive_snapshot_async(self, dataset: str, stdin: IO[bytes]) -> Popen[bytes]:
    cmd = ['zfs', 'receive', dataset]
    return self.start_command(cmd, stdin=stdin)

  # TrueNAS CORE 13.0 does not support holds -p, so we do not fetch timestamp
  def get_holds(self, snapshots_fullnames: Collection[str]) -> set[Hold]:
    lines = self.run_text_command(['zfs', 'holds', '-H', ' '.join(snapshots_fullnames)]).splitlines()
    holds: set[Hold] = set()
    for line in lines:
      fields = line.split('\t')
      holds.add(Hold(
        snapshot_fullname=fields[0],
        tag=fields[1]
      ))
    return holds
  
  def has_hold(self, snapshot_fullname: str, tag: str) -> bool:
    return any((s.tag == tag for s in self.get_holds([snapshot_fullname])))
  
  def hold(self, snapshots_fullnames: Collection[str], tag: str) -> None:
    self.run_text_command(['zfs', 'hold', tag, ' '.join(snapshots_fullnames)])

  def release(self, snapshots_fullnames: Collection[str], tag: str) -> None:
    self.run_text_command(['zfs', 'release', tag, ' '.join(snapshots_fullnames)])

  def get_pool_from_dataset(self, dataset: str) -> Pool:
    name = dataset.split('/')[0]
    guid = self.run_text_command(['zpool', 'get', '-Hp', '-o', 'value', 'guid', name])
    return Pool(name=name, guid=int(guid))

  def create_snapshot(self, dataset: str, short_name: str, recursive: bool = False) -> None:
    full_name = f'{dataset}@{short_name}'
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
        shortname = _short_name,
        timestamp = datetime.fromtimestamp(int(fields[1])),
        guid = int(fields[2])
      )
      snapshots.add(snap)

    return snapshots

  def destroy_snapshots(self, dataset: str, snapshots_shortnames: Collection[str]) -> None:
    shortnames_str = ','.join(snapshots_shortnames)
    try:
      self.run_text_command(['zfs', 'destroy', f'{dataset}@{shortnames_str}'])
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
