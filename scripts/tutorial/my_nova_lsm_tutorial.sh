#!/bin/bash
dryrun="false" #打出来看看
mydebug="true" #后面的东西 包括exec都打出来查看
recordcount="10000000" 

# bash /home/yuhang/NovaLSM/scripts/nova_lsm_tutorial_backup.sh $recordcount $dryrun > backup_out
bash /home/yuhang/NovaLSM/scripts/tutorial/my_nova_lsm_tutorial_exp.sh $recordcount $dryrun $mydebug > exp_out