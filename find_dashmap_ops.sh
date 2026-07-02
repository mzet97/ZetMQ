#!/bin/bash
cd /mnt/d/TI/git/ZetMQ
rg -F -n ".entry(" crates/zetmq-core/src crates/zetmq-server/src
echo "---"
rg -F -n ".insert(" crates/zetmq-core/src crates/zetmq-server/src
echo "---"
rg -F -n ".get_mut(" crates/zetmq-core/src crates/zetmq-server/src
echo "---"
rg -F -n ".remove(" crates/zetmq-core/src crates/zetmq-server/src
