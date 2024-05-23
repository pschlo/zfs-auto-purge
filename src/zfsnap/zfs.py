from __future__ import annotations
from datetime import datetime
from subprocess import Popen, PIPE, CalledProcessError
from typing import Optional, IO, Literal
from collections.abc import Collection, Iterable
import dataclasses
from dataclasses import dataclass
from enum import Enum


class ZfsProperty(Enum):
  NAME = 'name'
  CREATION = 'creation'
  GUID = 'guid'
  USERREFS = 'userrefs'
  CUSTOM_TAGS = 'zfsnap:tags'  # the user property used to store and read tags


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
    return dataclasses.replace(self, dataset=dataset)

  def with_shortname(self, shortname: str) -> Snapshot:
    return dataclasses.replace(self, shortname=shortname)


@dataclass(eq=True, frozen=True)
class Pool:
  name: str
  guid: int

@dataclass(eq=True, frozen=True)
class Dataset:
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
    return stdout
  
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
  
  def get_dataset(self, name: str) -> Dataset:
    guid = self.run_text_command(['zfs', 'get', '-Hp', '-o', 'value', 'guid', name])
    return Dataset(name=name, guid=int(guid))
  
  def get_datasets(self) -> list[Dataset]:
    P = ZfsProperty
    cmd = ['zfs', 'list', '-Hp', '-o', ','.join(p.value for p in P)]
    lines = self.run_text_command(cmd).splitlines()

    datasets: list[Dataset] = []
    for line in lines:
      fields = {p: v if v != '-' else '' for p, v in zip(P, line.split('\t'))}
      dataset = Dataset(
        name=fields[P.NAME],
        guid=int(fields[P.GUID])
      )
      datasets.append(dataset)
  
    return datasets

  def create_snapshot(self, fullname: str, recursive: bool = False, properties: dict[ZfsProperty, str] = dict()) -> None:
    cmd = ['zfs', 'snapshot']
    if recursive:
      cmd += ['-r']
    for property, value in properties.items():
      cmd += ['-o', f'{property.value}={value}']
    cmd += [fullname]
    self.run_text_command(cmd)
  
  def rename_snapshot(self, fullname: str, new_shortname: str) -> None:
    cmd = ['zfs', 'rename', fullname, new_shortname]
    self.run_text_command(cmd)

  def get_snapshot(self, fullname: str) -> Snapshot:
    P = ZfsProperty
    cmd = ['zfs', 'get', '-Hp', '-o', 'value', ','.join(p.value for p in P), fullname]
    lines = self.run_text_command(cmd).splitlines()
    fields = {p: v if v != '-' else '' for p, v in zip(P, lines)}
    dataset, shortname = fields[P.NAME].split('@')
    return Snapshot(
      dataset=dataset,
      shortname=shortname,
      timestamp=datetime.fromtimestamp(int(fields[P.CREATION])),
      guid=int(fields[P.GUID]),
      holds=int(fields[P.USERREFS]),
      tags=frozenset(fields[P.CUSTOM_TAGS].split(','))
    )

  def get_snapshots(self,
    dataset: Optional[str] = None,
    recursive: bool = False,
    sort_by: Optional[ZfsProperty] = None,
    reverse: bool = False
  ) -> list[Snapshot]:
    P = ZfsProperty
    cmd = ['zfs', 'list', '-Hp', '-t', 'snapshot', '-o', ','.join(p.value for p in P)]
    if recursive:
      cmd += ['-r']
    if sort_by is not None:
      cmd += ['-s' if not reverse else '-S', sort_by.value]
    if dataset:
      cmd += [dataset]
    lines = self.run_text_command(cmd).splitlines()

    snapshots: list[Snapshot] = []
    for line in lines:
      fields = {p: v if v != '-' else '' for p, v in zip(P, line.split('\t'))}
      dataset, shortname = fields[P.NAME].split('@')
      snap = Snapshot(
        dataset=dataset,
        shortname=shortname,
        timestamp=datetime.fromtimestamp(int(fields[P.CREATION])),
        guid=int(fields[P.GUID]),
        holds=int(fields[P.GUID]),
        tags=frozenset(fields[P.CUSTOM_TAGS].split(','))
      )
      snapshots.append(snap)

    return snapshots
  
  def set_tags(self, snap_fullname: str, tags: Collection[str]):
    cmd = ['zfs', 'set', f"{ZfsProperty.CUSTOM_TAGS.value}={','.join(tags)}", snap_fullname]
    self.run_text_command(cmd)

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
