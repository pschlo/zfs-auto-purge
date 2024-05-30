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
  src_tag = holdtags[0] if isinstance(holdtags[0], str) else holdtags[0](dest_cli.get_dataset(dest_dataset))
  dest_tag = holdtags[1] if isinstance(holdtags[1], str) else holdtags[1](src_cli.get_dataset(snapshot.dataset))
  src_cli.hold([snapshot.longname], src_tag)
  dest_cli.hold([snapshot.with_dataset(dest_dataset).longname], dest_tag)



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
    s = base.longname
    if unsafe_release or clis[0].has_hold(s, holdtags[0]):
      clis[0].release([s], holdtags[0])
    s = base.with_dataset(dest_dataset).longname
    if unsafe_release or clis[1].has_hold(s, holdtags[1]):
      clis[1].release([s], holdtags[1])
