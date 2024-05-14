from __future__ import annotations
from datetime import datetime
from subprocess import Popen, PIPE, CalledProcessError
from typing import Optional, IO
from collections.abc import Collection, Iterable
from dataclasses import dataclass

from .constants import TAGS_PROPERTY


@dataclass(eq=True, frozen=True)
class Snapshot:
  dataset: str
  shortname: str
  tags: frozenset[str]
  timestamp: datetime
  guid: int
  holds: int

  @property
  def longname(self):
    return f'{self.dataset}@{self.shortname}'
  
  def with_dataset(self, dataset: str) -> Snapshot:
    return Snapshot(
      dataset=dataset,
      shortname=self.shortname,
      timestamp=self.timestamp,
      tags=self.tags,
      guid=self.guid,
      holds=self.holds
    )


@dataclass(eq=True, frozen=True)
class Pool:
  name: str
  guid: int

@dataclass(eq=True, frozen=True)
class Hold:
  snap_longname: str
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
  def get_holds(self, snapshots_fullnames: Iterable[str]) -> set[Hold]:
    lines = self.run_text_command(['zfs', 'holds', '-H', *snapshots_fullnames]).splitlines()
    holds: set[Hold] = set()
    for line in lines:
      snapname, tag, _ = line.split('\t')
      holds.add(Hold(
        snap_longname=snapname,
        tag=tag
      ))
    return holds
  
  def has_hold(self, snapshot_fullname: str, tag: str) -> bool:
    """Convenience method for checking if snapshot has hold with certain name"""
    return any((s.tag == tag for s in self.get_holds([snapshot_fullname])))
  
  def hold(self, snapshots_fullnames: Collection[str], tag: str) -> None:
    self.run_text_command(['zfs', 'hold', tag, ' '.join(snapshots_fullnames)])

  def release(self, snapshots_fullnames: Collection[str], tag: str) -> None:
    self.run_text_command(['zfs', 'release', tag, ' '.join(snapshots_fullnames)])

  def get_pool_from_dataset(self, dataset: str) -> Pool:
    name = dataset.split('/')[0]
    guid = self.run_text_command(['zpool', 'get', '-Hp', '-o', 'value', 'guid', name])
    return Pool(name=name, guid=int(guid))

  def create_snapshot(self, dataset: str, name: str, recursive: bool = False, properties: dict[str, str] = dict()) -> None:
    longname = f'{dataset}@{name}'
    cmd = ['zfs', 'snapshot']
    if recursive:
      cmd += ['-r']
    for property, value in properties.items():
      cmd += ['-o', f'{property}={value}']
    cmd += [longname]
    self.run_text_command(cmd)

  def get_snapshots(self, dataset: Optional[str] = None, recursive: bool = False) -> set[Snapshot]:
    properties: list[str] = ['name', 'creation', 'guid', 'userrefs', TAGS_PROPERTY]
    cmd = ['zfs', 'list', '-Hp', '-t', 'snapshot', '-o', ','.join(properties)]
    if recursive:
      cmd += ['-r']
    if dataset:
      cmd += [dataset]
    lines = self.run_text_command(cmd).splitlines()
    snapshots: set[Snapshot] = set()

    for line in lines:
      name, creation, guid, userrefs, tags = line.split('\t')
      dataset, shortname = name.split('@')
      # no tags
      if tags == '-':
        tags = ''
      snap = Snapshot(
        dataset=dataset,
        shortname=shortname,
        timestamp=datetime.fromtimestamp(int(creation)),
        guid=int(guid),
        holds=int(userrefs),
        tags=frozenset(tags.split(','))
      )
      snapshots.add(snap)

    return snapshots

  def destroy_snapshots(self, dataset: str, snapshots_shortnames: Collection[str]) -> None:
    shortnames_str = ','.join(snapshots_shortnames)
    self.run_text_command(['zfs', 'destroy', f'{dataset}@{shortnames_str}'])


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
