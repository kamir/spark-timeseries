/**
 *   EXTRACT substrings via Regular Expressions ... 
 */

//
// Run the SparkShell in directory which contains the files
//
// - mahout-core-0.9.jar
// - mahout-math-0.9.jar 
// - handleLabels.scala
//
// by this command
// 
// > ADD_JARS=mahout-core-0.9.jar,mahout-math-0.9.jar spark-shell
// 
// then run in the SparkShell (which is build on the Scala REPL)
// the command: 
// scala> :load handleLabels.scala
//

import org.apache.mahout.math.VectorWritable
import org.apache.mahout.math.Vector
import org.apache.hadoop.io.Text
import java.net.URLEncoder


// What is the name of the study we work on?
val studyname: String = "dissertation_DEMO"

// Pattern to split the KEYs, stored in the SEQUNECE files
val GROUPa = """(.+).accessrate_(.+)""".r
val GROUPe = """(.+).editrate_(.+)""".r
val GROUPe2 = """(.+)%23%23%23(.+)""".r

// Some values for testing ... 
//val k1: String = "2_A.L.accessrate_en___AVUS"
//val k2: String = "2_A.L.editrate_en___AVUS"
//var k2: String = "dissertation_DEMO_2_A.L.editrate_en___First Racing"
//var k3: String = "dissertation_DEMO_2_A.L.editrate_en___(First, Racing)"

//
// Do some testing on the KEY extraction part
//
//val expr = "[ (),]".r   
//k2 = expr.replaceAllIn(k2, "_") 

//
// Pattern matching fails if " " is in the key, so we replace it.
//
//def cleanKey2 ( k:String ) : String = { expr.replaceAllIn(k, "_") } 
def cleanKey ( k:String ) : String = { URLEncoder.encode(k) } 

//println( "Clean Key  : " + cleanKey( k2 ) )
//println( "Clean Key  : " + cleanKey( k3 ) )

//println( "Clean Key 2: " + cleanKey2( k2 ) )
//println( "Clean Key 2: " + cleanKey2( k3 ) )


def sumVector (v:VectorWritable) : Double = { 
	v.get().zSum()
}


def getGroupACCESS (l:String) : String = { 
   cleanKey( l ) match {
      case GROUPa(gr, page) => (gr)
   }
}

def getGroupEDITS (l:String) : String = {
   cleanKey( l ) match {
      case GROUPe(gr, page) => (gr)
   }
}

def getPageACCESS (l:String) : String = {
   cleanKey( l ) match {
      case GROUPa(gr, page) => (page)
   }
}

def getPageEDITS (l:String) : String = {
   cleanKey( l ) match {
      case GROUPe(gr, page) => (page)
   }
}

// Here I provide the values which come in
def getPageEDITS2 (l:String) : String = {
   cleanKey( l ) match {
      case GROUPe2(gr, page) => (page)
   }
}


//
// process a pair of group ...
//
def processACN( NR:String, GR1:String, GR2:String ) : String = {

// prepare directories ...
val inPath = "file:///mnt/hgfs/SHARE.VM.MAC/DATA/by_CN_AND_Group/"


val fileNameEr1 = inPath + studyname + "_" + NR + "_" + GR1 + ".editrate.tsb.seq"
val fileNameEr2 = inPath + studyname + "_" + NR + "_" + GR2 + ".editrate.tsb.seq"

val fileNameAr1 = inPath + NR + "_" + GR1 + ".accessrate.tsb.seq"
val fileNameAr2 = inPath + NR + "_" + GR2 + ".accessrate.tsb.seq"

val outPath = "file:///mnt/hgfs/SHARE.VM.MAC/DATA/OUT/" + NR + "." + GR1 + "." + GR2 + ".a_e_sum.csv"

// define the data source 
val tsbEditsRDD1 = sc.sequenceFile(fileNameEr1,classOf[Text], classOf[VectorWritable])
val tsbAccessRDD1 = sc.sequenceFile(fileNameAr1,classOf[Text], classOf[VectorWritable])
val tsbEditsRDD2 = sc.sequenceFile(fileNameEr2,classOf[Text], classOf[VectorWritable])
val tsbAccessRDD2 = sc.sequenceFile(fileNameAr2,classOf[Text], classOf[VectorWritable])

val tsbEditsRDD = tsbEditsRDD1.union( tsbEditsRDD2 )
val tsbAccessRDD = tsbAccessRDD1.union( tsbAccessRDD2 )

// How many elements are available?
// tsbEditsRDD.count()
// tsbAccessRDD.count()

// materialize the RDD and take the first element 
// val e = tsbEditsRDD.take(1)
// val a = tsbAccessRDD.take(1)

// here we find out the structure of the individual elementes of the RDD, which is a Tuple2 in this case
// println( "Edits:  " + e )
// println( "Access: " + a )

val ets = tsbEditsRDD.map( ts => ( getPageEDITS( ts._1.toString() ), sumVector( ts._2 ) ) )

val ats = tsbAccessRDD.map( ts => ( getPageACCESS( ts._1.toString() ), sumVector( ts._2 ) ) )

val both = ets.join( ats, 1 )

both.cache()

ets.take(1)

ats.take(1)

both.take(1)

val finalResult = both.map { 
  //x => ( x._1,( x._2._1,x._2._2) )
  rec => ( rec._2._1 + " " + rec._2._2 )
}

// both.saveAsTextFile( resultFoldeName )

finalResult.saveAsTextFile( outPath )

"Results stored in: " + outPath

}




// the STREAMER TOOL produces the wrong KEYs ... so we need another
// Key Splitter ...


//
// process a pair of group ...
//
def processACNV2( NR:String, GR1:String, GR2:String ) : String = {

// prepare directories ...
val inPath = "file:///mnt/hgfs/SHARE.VM.MAC/DATA/by_CN_AND_Group/"

val fileNameEr1 = inPath + studyname + "_" + NR + "_" + GR1 + ".editrate.tsb.seq"  // A.L
val fileNameEr2 = inPath + studyname + "_" + NR + "_" + GR2 + ".editrate.tsb.seq"  // B.L

val fileNameAr1 = inPath + NR + "_" + GR1 + ".accessrate.tsb.seq"  // A.L
val fileNameAr2 = inPath + NR + "_" + GR2 + ".accessrate.tsb.seq"  // B.L

val outPath = "file:///mnt/hgfs/SHARE.VM.MAC/DATA/OUT/" + NR + "." + GR1 + "." + GR2 + ".a_e_sum.csv"

// define the data source 
val tsbEditsRDD1 = sc.sequenceFile(fileNameEr1,classOf[Text], classOf[VectorWritable])
val tsbAccessRDD1 = sc.sequenceFile(fileNameAr1,classOf[Text], classOf[VectorWritable])
val tsbEditsRDD2 = sc.sequenceFile(fileNameEr2,classOf[Text], classOf[VectorWritable])
val tsbAccessRDD2 = sc.sequenceFile(fileNameAr2,classOf[Text], classOf[VectorWritable])

//
//  Here I need a new KEY-Extractor ...
//
val ets1 = tsbEditsRDD1.map( ts => ( getPageEDITS( ts._1.toString() ), sumVector( ts._2 ) ) )
val ets2 = tsbEditsRDD2.map( ts => ( getPageEDITS2( ts._1.toString() ), sumVector( ts._2 ) ) )

val ats2 = tsbAccessRDD2.map( ts => ( getPageACCESS( ts._1.toString() ), sumVector( ts._2 ) ) )
val ats1 = tsbAccessRDD1.map( ts => ( getPageACCESS( ts._1.toString() ), sumVector( ts._2 ) ) )

val ets = ets1.union( ets2 )
val ats = ats1.union( ats2 )


//ets1.saveAsTextFile( "file:///mnt/hgfs/SHARE.VM.MAC/DATA/OUT/ets1.csv" )
//ets2.saveAsTextFile( "file:///mnt/hgfs/SHARE.VM.MAC/DATA/OUT/ets2.csv" )
//ats1.saveAsTextFile( "file:///mnt/hgfs/SHARE.VM.MAC/DATA/OUT/ats1.csv" )
//ats2.saveAsTextFile( "file:///mnt/hgfs/SHARE.VM.MAC/DATA/OUT/ats2.csv" )

val both = ets.join( ats, 1 )
//val both = ets.leftOuterJoin( ats, 1 )

//both.cache()

//println( "<<<  EDITS >>> " + ets.take(1) )
//println( "<<< ACCESS >>> " + ats.take(1) )

val finalResult = both.map { 
  //x => ( x._1,( x._2._1,x._2._2) )
  rec => ( rec._2._1 + " " + rec._2._2 )
}

// both.saveAsTextFile( resultFoldeName )

finalResult.saveAsTextFile( outPath )

//println( "EDIT   rows: " + ez )
//println( "ACCESS rows: " + az )

"Results stored in: " + outPath

}















// MAIN program ...
println ( processACN( "2", "CN","IWL" ) )
println ( processACN( "4", "CN","IWL" ) )

println ( processACNV2( "2", "A.L","B.L" ) )
println ( processACNV2( "4", "A.L","B.L" ) )







