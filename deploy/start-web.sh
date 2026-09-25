#!/usr/bin/env bash
# The trading web app (Next.js) as a launchd service: a production build,
# then `next start` on 3010 (see web/package.json). Rebuilds on every
# start, so a launchd restart always serves the checked-out code.
# Usage: start-web.sh /absolute/path/to/npm  (filled in by install-live-stack.sh)
set -euo pipefail

NPM="$1"
export PATH="$(dirname "$NPM"):$PATH"  # npm's `#!/usr/bin/env node` shebang needs node

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/web"
"$NPM" run build
exec "$NPM" run start
