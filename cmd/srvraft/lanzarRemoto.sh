#!/bin/bash

ssh 155.210.154.194 "cd /home/a801618/3º/SSDD/raft/cmd/srvraft; ./srvraft 0 155.210.154.194:17561 155.210.154.197:17562 155.210.154.198:17563"&
ssh 155.210.154.197 "cd /home/a801618/3º/SSDD/raft/cmd/srvraft; ./srvraft 1 155.210.154.194:17561 155.210.154.197:17562 155.210.154.198:17563"&
ssh 155.210.154.198 "cd /home/a801618/3º/SSDD/raft/cmd/srvraft; ./srvraft 2 155.210.154.194:17561 155.210.154.197:17562 155.210.154.198:17563"&

echo "FIN"
