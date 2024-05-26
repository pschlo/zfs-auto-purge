from typing import Optional, Callable, Union
from subprocess import CalledProcessError
import time

from ..zfs import ZfsCli, Snapshot, ZfsProperty, Dataset

Holdtag = Union[str, Callable[[Dataset],str]]


def _send_receive(
  clis: tuple[ZfsCli, ZfsCli],
  dest_dataset: str,
  snapshot: Snapshot,
  base: Optional[Snapshot],
  holdtags: tuple[Holdtag,Holdtag],
  properties: dict[str, str] = {}
) -> None:
  src_cli, dest_cli = clis

  # create sending and receiving process
  send_proc = src_cli.send_snapshot_async(snapshot.longname, base.longname if base else None)
  assert send_proc.stdout is not None
  recv_proc = dest_cli.receive_snapshot_async(dest_dataset, send_proc.stdout, properties)
  
  # wait for both processes to terminate
  while True:
    send_status, recv_status = send_proc.poll(), recv_proc.poll()
    if send_status is not None and recv_status is not None:
      # both terminated
      break
    if send_status is not None and send_status > 0:
      # zfs send process died with error
      recv_proc.terminate()
    if recv_status is not None and recv_status > 0:
      # zfs receive process died with error
      send_proc.terminate()
    time.sleep(0.1)

  # check exit codes
  for p in send_proc, recv_proc:
    if p.returncode > 0:
      raise CalledProcessError(p.returncode, cmd=p.args)
    
  # set tags on dest snapshot
  if snapshot.tags is not None:
    dest_cli.set_tags(snapshot.with_dataset(dest_dataset).longname, snapshot.tags)

  # hold snaps
  _hold(src_cli, snapshot, holdtags[0])
  _hold(dest_cli, snapshot.with_dataset(dest_dataset), holdtags[1])
  


def _hold(cli: ZfsCli, snap: Snapshot, holdtag: Holdtag):
  tag = holdtag if isinstance(holdtag, str) else holdtag(cli.get_dataset(snap.dataset))
  cli.hold([snap.longname], tag)

def _release(cli: ZfsCli, snap: Snapshot, holdtag: Holdtag, unsafe: bool=False):
  tag = holdtag if isinstance(holdtag, str) else holdtag(cli.get_dataset(snap.dataset))
  if unsafe or cli.has_hold(snap.longname, tag):
    cli.release([snap.longname], tag)



def send_receive_initial(
  clis: tuple[ZfsCli, ZfsCli],
  dest_dataset: str,
  snapshot: Snapshot,
  holdtags: tuple[Callable[[Dataset], str], Callable[[Dataset], str]]
) -> None:
  _send_receive(
    clis=clis,
    dest_dataset=dest_dataset,
    snapshot=snapshot,
    base=None,
    holdtags=holdtags,
    properties={
      ZfsProperty.READONLY: 'on',
      ZfsProperty.ATIME: 'off'
    },
  )
  

def send_receive_incremental(
  clis: tuple[ZfsCli, ZfsCli],
  dest_dataset: str,
  holdtags: tuple[str,str],
  snapshot: Snapshot,
  base: Optional[Snapshot]=None,
  unsafe_release: bool=False
) -> None:
  _send_receive(
    clis=clis,
    dest_dataset=dest_dataset,
    snapshot=snapshot,
    base=base,
    holdtags=holdtags
  )
  # release base snaps
  if base:
    _release(clis[0], base, holdtags[0], unsafe=unsafe_release)
    _release(clis[1], base.with_dataset(dest_dataset), holdtags[1], unsafe=unsafe_release)
