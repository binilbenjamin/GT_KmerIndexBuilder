GT_KmerIndexBuilder
===================

K-mer index builder using Hadoop/GenomeTools


To run, build the jar file and run it on a Hadoop cluster. <br/>
An example command :
```
hadoop jar GT_KmerIndexBuilder.jar edu.arizona.cs.gt.kmer.Main -D gt.loc=/home/binilbenjamin/gt-1.5.1-Linux_x86_64-64bit/bin/gt -D gt.tallymer.mersize=20 -D gt.suffixerator.parts=4 -D gt.suffixerator.indexname=sreads -D gt.tallymer.indexname=kreads -D mapred.task.timeout=1800000 /home/binilbenjamin/run/input/POV_*.gz /home/binilbenjamin/run/index
```
