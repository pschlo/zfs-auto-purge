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
  source_cli, dest_cli = clis
  hold_tag_source, hold_tag_dest = hold_tags

  send_proc = source_cli.send_snapshot_async(snapshot.fullname, base_fullname=base.fullname if base else None)
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
  source_cli.hold([snapshot.fullname], hold_tag_source)
  dest_cli.hold([snapshot.with_dataset(dest_dataset).fullname], hold_tag_dest)

  # release base snaps
  if base is None:
    return
  s = base
  if unsafe_release or source_cli.has_hold(s.fullname, hold_tag_source):
    source_cli.release([s.fullname], hold_tag_source)
  s = base.with_dataset(dest_dataset)
  if unsafe_release or dest_cli.has_hold(s.fullname, hold_tag_dest):
    dest_cli.release([s.fullname], hold_tag_dest)
