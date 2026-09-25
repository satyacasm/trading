# Compute the PATH a launchd agent needs so colima (start-colima.sh) and
# docker (Popen in agent_contract/sandbox.py) resolve. launchd's own PATH
# is only /usr/bin:/bin:/usr/sbin:/sbin, so without this every plist that
# shells out to colima or docker fails silently once installed (C1).
#
# Sourced (not executed) by install-live-stack.sh and by tests, which run
# it against a fake PATH -- neither ever invokes docker/colima/uv here,
# they only need to be findable with `command -v`.
resolve_launchd_path() {
  local docker_bin colima_bin uv_bin
  docker_bin="$(command -v docker || true)"
  colima_bin="$(command -v colima || true)"
  uv_bin="$(command -v uv || true)"

  if [ -z "$docker_bin" ] || [ -z "$colima_bin" ] || [ -z "$uv_bin" ]; then
    echo "docker, colima, and uv must all be on PATH before installing the live stack" >&2
    return 1
  fi

  echo "$(dirname "$docker_bin"):$(dirname "$colima_bin"):$(dirname "$uv_bin"):/usr/bin:/bin:/usr/sbin:/sbin"
}
