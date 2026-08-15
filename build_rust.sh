#!/bin/bash
# newfreedeck_core — 编译 + 复制到 py_modules/
set -e
cd "$(dirname "$0")/newfreedeck_core"

# 确保 symlink 存在（libgcc_s → libgcc）
for lib in gcc pthread dl rt util; do
    case $lib in
        gcc) target=/usr/lib/libgcc_s.so.1 ;;
        *)   target=/usr/lib/libc.so ;;
    esac
    [ -e "lib${lib}.so" ] || ln -sf "$target" "lib${lib}.so"
done

env CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=/usr/bin/ld.bfd \
    RUSTFLAGS="-L /usr/lib -L ." \
    cargo rustc --release -- -C link-args="--no-as-needed -lgcc_s"

cp -f target/release/libnewfreedeck_core.so ../py_modules/
echo "✅ $(ls -lh ../py_modules/libnewfreedeck_core.so | awk '{print $5}')"
