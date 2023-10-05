#!/bin/bash
dryrun="true"

recordcount="10000000"

# bash /home/yuhang/NovaLSM/scripts/nova_lsm_tutorial_backup.sh $recordcount $dryrun > backup_out
bash /home/yuhang/NovaLSM/scripts/tutorial/nova_lsm_tutorial_exp.sh $recordcount $dryrun > exp_out
