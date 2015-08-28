import org.apache.mahout.math.VectorWritable
import org.apache.hadoop.io.Text
val tsbRDD = sc.sequenceFile("file:///mnt/hgfs/SHARE.VM.MAC/DATA/EditEvents_1262304000000_1293753600000_tutorial_one.tsb.seq",classOf[Text], classOf[VectorWritable])
tsbRDD.count();