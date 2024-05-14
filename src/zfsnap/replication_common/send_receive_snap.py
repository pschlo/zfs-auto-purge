from typing import Optional
from subprocess import CalledProcessError
import time

from ..zfs import ZfsCli, Snapshot


def send_receive(
  clis: tuple[ZfsCli, ZfsCli],
  dest_dataset: str,
  hold_tags: tuple[str,str],
  snapshot: Snapshot,
  base: Optional[Snapshot]=None,
  unsafe_release: bool=False
) -> None:
  src_cli, dest_cli = clis
  src_holdtag, dest_holdtag = hold_tags
  src_snap, dest_snap = snapshot, snapshot.with_dataset(dest_dataset)
  src_base, dest_base = base, base.with_dataset(dest_dataset) if base else None
  del snapshot

  send_proc = src_cli.send_snapshot_async(src_snap.longname, base_fullname=src_base.longname if src_base else None)
  assert send_proc.stdout is not None
  recv_proc = dest_cli.receive_snapshot_async(dest_dataset, stdin=send_proc.stdout)
  
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

  for p in send_proc, recv_proc:
    if p.returncode > 0:
      raise CalledProcessError(p.returncode, cmd=p.args)
    
  # hold snaps
  src_cli.hold([src_snap.longname], src_holdtag)
  dest_cli.hold([dest_snap.longname], dest_holdtag)

  # set tags on dest snapshot
  dest_cli.set_tags(dest_snap.longname, src_snap.tags)

  # release base snaps
  if src_base is None or dest_base is None:
    return
  if unsafe_release or src_cli.has_hold(src_base.longname, src_holdtag):
    src_cli.release([src_base.longname], src_holdtag)
  if unsafe_release or dest_cli.has_hold(dest_base.longname, dest_holdtag):
    dest_cli.release([dest_base.longname], dest_holdtag)
