#!/usr/bin/env bash
# 배포용 바이너리(dist 프로파일)로 RPM을 만든다. rpm-build 패키지가 필요하다.
#   RELEASE=8 scripts/build-rpm.sh
# 결과: ~/rpmbuild/RPMS/x86_64/eroundb-<ver>-<rel>.el9.x86_64.rpm 와 dist/eroundb-<ver>-<rel>.manifest
# (빌드 ID, 소스 트리 SHA-256, 바이너리·RPM SHA-256 대응표)
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
VERSION="$(grep -m1 '^version' "$ROOT/Cargo.toml" | cut -d'"' -f2)"
RELEASE="${RELEASE:-8}"
cd "$ROOT"
# 소스 트리 해시: 빌드에 들어가는 파일(Rust 소스, Cargo, 설정, 패키징) 내용 기준
INPUTS=(Cargo.toml Cargo.lock rust-toolchain.toml crates bin config packaging)
if git rev-parse --git-dir >/dev/null 2>&1; then
  list_inputs() { git ls-files -co --exclude-standard -- "${INPUTS[@]}"; }
  GIT_REV="$(git rev-parse --short HEAD 2>/dev/null || echo nogit)"
  TREE_STATE="$(git status --porcelain | wc -l) changed/untracked paths"
else  # GitHub zip/tarball 등 .git 이 없는 소스 트리: 같은 파일 집합을 find 로 나열한다
  list_inputs() { find "${INPUTS[@]}" -type f; }
  GIT_REV=nogit
  TREE_STATE="no git metadata"
fi
SRC_SHA="$( { list_inputs | LC_ALL=C sort | while read -r f; do [ -f "$f" ] && sha256sum "$f"; done; } | sha256sum | cut -c1-16)"
export JAMSIL_BUILD_ID="$GIT_REV+src.$SRC_SHA"
export JAMSIL_RELEASE="$RELEASE"
cargo build --profile dist
STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT
PKG="$STAGE/eroundb-$VERSION"
mkdir -p "$PKG/bin" "$PKG/config" "$PKG/packaging/systemd"
for b in jamsild jamsil-sql jamsil-store jamsil-sync jamsil-search jamsil-log; do
  cp "target/dist/$b" "$PKG/bin/"
done
cp config/standalone.toml config/cluster.toml "$PKG/config/"
cp packaging/systemd/jamsildb.service packaging/systemd/jamsildb.sysusers.conf "$PKG/packaging/systemd/"
mkdir -p "$PKG/docs"
cp README.md LICENSE "$PKG/"
cp docs/REFERENCE.md docs/DESIGN.md "$PKG/docs/"
mkdir -p "$HOME/rpmbuild/SOURCES" "$HOME/rpmbuild/SPECS"
tar -C "$STAGE" -czf "$HOME/rpmbuild/SOURCES/eroundb-$VERSION-bin.tar.gz" "eroundb-$VERSION"
rpmbuild -bb --define "jamsil_version $VERSION" --define "jamsil_release $RELEASE" packaging/rpm/eroundb.spec
RPM="$(ls -1 "$HOME"/rpmbuild/RPMS/x86_64/eroundb-"$VERSION-$RELEASE".*.rpm | tail -1)"
mkdir -p dist
MAN="dist/eroundb-$VERSION-$RELEASE.manifest"
# 설치되는 파일 그대로의 해시 (rpmbuild가 strip하므로 target/dist 의 해시와 다르다)
X="$(mktemp -d)"; trap 'rm -rf "$X"' EXIT
(cd "$X" && rpm2cpio "$RPM" | cpio -idm --quiet './usr/bin/*')
{
  echo "package      $(basename "$RPM")"
  echo "version      $VERSION-$RELEASE"
  echo "build_id     $JAMSIL_BUILD_ID"
  echo "git_head     $GIT_REV (working tree: $TREE_STATE)"
  echo "source_sha   $SRC_SHA (first 16 hex of SHA-256 over build inputs)"
  echo "rustc        $(rustc --version)"
  echo "built_at     $(date -u +%Y-%m-%dT%H:%M:%SZ) on $(hostname)"
  echo "rpm_sha256   $(sha256sum "$RPM" | cut -d' ' -f1)"
  for b in jamsild jamsil-sql jamsil-store jamsil-sync jamsil-search jamsil-log; do
    printf '%-12s %s  /usr/bin/%s\n' "$b" "$(sha256sum "$X/usr/bin/$b" | cut -d' ' -f1)" "$b"
  done
} > "$MAN"
cat "$MAN"
echo "$RPM"
