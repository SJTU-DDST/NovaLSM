log文件的buf都是 max stoc file size的大小 18MB 很多时候都直接申请18MB 
manifest文件是64mb

元数据存储于额外的文件
sstable meta的大小，以及sstable的大小，parity就当没有 log = sstable
分配额度只按manifest sstable meta 和 sstable