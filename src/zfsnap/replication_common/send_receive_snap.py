from typing import Optional
from subprocess import CalledProcessError
import time

from ..zfs import ZfsCli, Snapshot, ZfsProperty


def _send_receive(
  clis: tuple[ZfsCli, ZfsCli],
  dest_dataset: str,
  snapshot: Snapshot,
  base: Optional[Snapshot],
  properties: dict[ZfsProperty, str] = {}
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
  dest_cli.set_tags(snapshot.with_dataset(dest_dataset).longname, snapshot.tags)


def send_receive_initial(
  clis: tuple[ZfsCli, ZfsCli],
  dest_dataset: str,
  snapshot: Snapshot,
) -> None:
  _send_receive(
    clis=clis,
    dest_dataset=dest_dataset,
    snapshot=snapshot,
    base=None,
    properties={
      ZfsProperty.READONLY: 'on',
      ZfsProperty.ATIME: 'off'
    }
  )
  

def send_receive_incremental(
  clis: tuple[ZfsCli, ZfsCli],
  dest_dataset: str,
  hold_tags: tuple[str,str],
  snapshot: Snapshot,
  base: Optional[Snapshot]=None,
  unsafe_release: bool=False
) -> None:
  _send_receive(
    clis=clis,
    dest_dataset=dest_dataset,
    snapshot=snapshot,
    base=base
  )
  
  # (re)define own variables
  src_cli, dest_cli = clis
  src_holdtag, dest_holdtag = hold_tags
  src_snap, dest_snap = snapshot, snapshot.with_dataset(dest_dataset)
  src_base, dest_base = base, base.with_dataset(dest_dataset) if base else None
  del clis, hold_tags, snapshot, base

  # hold snaps
  src_cli.hold([src_snap.longname], src_holdtag)
  dest_cli.hold([dest_snap.longname], dest_holdtag)

  # release base snaps
  if src_base and dest_base:
    if unsafe_release or src_cli.has_hold(src_base.longname, src_holdtag):
      src_cli.release([src_base.longname], src_holdtag)
    if unsafe_release or dest_cli.has_hold(dest_base.longname, dest_holdtag):
      dest_cli.release([dest_base.longname], dest_holdtag)
