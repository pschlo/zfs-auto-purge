from __future__ import annotations
from datetime import datetime
from subprocess import Popen, PIPE, CalledProcessError
from typing import Optional, IO, Literal
from collections.abc import Collection
from dataclasses import dataclass


class ZfsProperty:
  NAME = 'name'
  CREATION = 'creation'
  GUID = 'guid'
  USERREFS = 'userrefs'
  READONLY = 'readonly'
  ATIME = 'atime'
  CUSTOM_TAGS = 'zfsnappr:tags'  # the user property used to store and read tags


# properties that will always be fetched
REQUIRED_PROPS = [ZfsProperty.NAME, ZfsProperty.CREATION, ZfsProperty.GUID, ZfsProperty.CUSTOM_TAGS, ZfsProperty.USERREFS]


class Snapshot:
  properties: dict[str, str]

  dataset: str
  shortname: str
  guid: int
  timestamp: datetime
  tags: Optional[set[str]]
  holds: int

  def __init__(self, properties: dict[str,str]):
    P = ZfsProperty
    ps = properties

    self.properties = ps
    self.dataset, self.shortname = ps[P.NAME].split('@')
    self.guid = int(ps[P.GUID])
    self.timestamp = datetime.fromtimestamp(int(ps[P.CREATION]))
    self.holds = int(ps[P.USERREFS])

    if ps[P.CUSTOM_TAGS] == '-':
      self.tags = None
    else:
      self.tags = set(t for t in ps[P.CUSTOM_TAGS].split(',') if t)  # ignore empty tags

  def __repr__(self) -> str:
    return f"Snapshot({self.properties})"

  @property
  def longname(self):
    return f'{self.dataset}@{self.shortname}'
  
  def with_dataset(self, dataset: str) -> Snapshot:
    new_props = self.properties.copy()
    new_props[ZfsProperty.NAME] = f"{dataset}@{self.shortname}"
    return Snapshot(new_props)

  def with_shortname(self, shortname: str) -> Snapshot:
    new_props = self.properties.copy()
    new_props[ZfsProperty.NAME] = f"{self.dataset}@{shortname}"
    return Snapshot(new_props)


@dataclass(eq=True, frozen=True)
class Pool:
  name: str
  guid: int

class Dataset:
  properties: dict[str, str]

  name: str
  guid: int

  def __init__(self, properties: dict[str,str]):
    P = ZfsProperty
    ps = properties

    self.properties = ps
    self.name = ps[P.NAME]
    self.guid = int(ps[P.GUID])

  def __repr__(self) -> str:
    return f"Dataset({self.properties})"

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
    if base_fullname:
      cmd += ['-i', base_fullname]
    cmd += [snapshot_fullname]
    return self.start_command(cmd, stdout=PIPE)
  
  def receive_snapshot_async(self, dataset: str, stdin: IO[bytes], properties: dict[str, str] = {}) -> Popen[bytes]:
    cmd = ['zfs', 'receive']
    for property, value in properties.items():
      cmd += ['-o', f'{property}={value}']
    cmd += [dataset]
    return self.start_command(cmd, stdin=stdin)

  # TrueNAS CORE 13.0 does not support holds -p, so we do not fetch timestamp
  def get_holds(self, snapshots_fullnames: Collection[str]) -> set[Hold]:
    if not snapshots_fullnames:
      return set()
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
    if not snapshots_fullnames:
      return
    self.run_text_command(['zfs', 'hold', tag, *snapshots_fullnames])

  def release(self, snapshots_fullnames: Collection[str], tag: str) -> None:
    if not snapshots_fullnames:
      return
    self.run_text_command(['zfs', 'release', tag, *snapshots_fullnames])

  def get_pool_from_dataset(self, dataset: str) -> Pool:
    name = dataset.split('/')[0]
    guid = self.run_text_command(['zpool', 'get', '-Hp', '-o', 'value', 'guid', name])
    return Pool(name=name, guid=int(guid))
  
  def get_datasets(self, names: Collection[str], properties: Collection[str] = []) -> list[Dataset]:
    if not names:
      return []
    properties = list(dict.fromkeys(REQUIRED_PROPS + list(properties)))  # eliminate duplicates
    
    cmd = ['zfs', 'get', '-Hp', '-o', 'value', ','.join(properties), *names]
    lines = self.run_text_command(cmd).splitlines()

    datasets: list[Dataset] = []
    for i in range(len(names)):
      props = {p: v for p, v in zip(properties, lines[i*len(properties):(i+1)*len(properties)])}
      datasets.append(Dataset(props))
    return datasets
  

  def get_dataset(self, name: str, properties: Collection[str] = []) -> Dataset:
    """Shorthand method"""
    return next(iter(self.get_datasets([name], properties)))

  
  def get_all_datasets(self, properties: Collection[str] = []) -> list[Dataset]:
    properties = list(dict.fromkeys(REQUIRED_PROPS + list(properties)))  # eliminate duplicates

    cmd = ['zfs', 'list', '-Hp', '-o', ','.join(properties)]
    lines = self.run_text_command(cmd).splitlines()

    datasets: list[Dataset] = []
    for line in lines:
      props = {p: v for p, v in zip(properties, line.split('\t'))}
      datasets.append(Dataset(props))
  
    return datasets
  
  def create_snapshot(self, fullname: str, recursive: bool = False, properties: dict[str, str] = {}) -> None:
    cmd = ['zfs', 'snapshot']
    if recursive:
      cmd += ['-r']
    for property, value in properties.items():
      cmd += ['-o', f'{property}={value}']
    cmd += [fullname]
    self.run_text_command(cmd)
  
  def rename_snapshot(self, fullname: str, new_shortname: str) -> None:
    cmd = ['zfs', 'rename', fullname, new_shortname]
    self.run_text_command(cmd)

  def get_snapshots(self, fullnames: Collection[str], properties: Collection[str] = []) -> list[Snapshot]:
    if not fullnames:
      return []
    properties = list(dict.fromkeys(REQUIRED_PROPS + list(properties)))  # eliminate duplicates
    
    cmd = ['zfs', 'get', '-Hp', '-o', 'value', ','.join(properties), *fullnames]
    lines = self.run_text_command(cmd).splitlines()

    snaps: list[Snapshot] = []
    for i in range(len(fullnames)):
      props = {p: v for p, v in zip(properties, lines[i*len(properties):(i+1)*len(properties)])}
      snaps.append(Snapshot(props))
    return snaps

  def get_all_snapshots(self,
    dataset: Optional[str] = None,
    recursive: bool = False,
    properties: Collection[str] = [],
    sort_by: Optional[str] = None,
    reverse: bool = False
  ) -> list[Snapshot]:
    properties = list(dict.fromkeys(REQUIRED_PROPS + list(properties)))  # eliminate duplicates

    cmd = ['zfs', 'list', '-Hp', '-t', 'snapshot', '-o', ','.join(properties)]
    if recursive:
      cmd += ['-r']
    if sort_by is not None:
      cmd += ['-s' if not reverse else '-S', sort_by]
    if dataset:
      cmd += [dataset]
    lines = self.run_text_command(cmd).splitlines()

    snapshots: list[Snapshot] = []
    for line in lines:
      props = {p: v for p, v in zip(properties, line.split('\t'))}
      snapshots.append(Snapshot(props))

    return snapshots

  
  def set_tags(self, snap_fullname: str, tags: Collection[str]):
    cmd = ['zfs', 'set', f"{ZfsProperty.CUSTOM_TAGS}={','.join(tags)}", snap_fullname]
    self.run_text_command(cmd)

  def destroy_snapshots(self, dataset: str, snapshots_shortnames: Collection[str]) -> None:
    if not snapshots_shortnames:
      return
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
