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

val groupname: String = "dissertation_DEMO.Formula_one.2_A.L"

//
// Example 1 : Decomposition Extract an Email
//
val Email = """([-0-9a-zA-Z.+_]+)@K([-0-9a-zA-Z.+_]+)\.([a-zA-Z]{2,4})""".r

"user@Kdomain.com" match {
    case Email(name, domain, zone) =>
       println(name)
       println(domain)
       println(zone)
}

import java.net.URLEncoder

//
// Example 2 : Decomposition of object-identifiers from time series buckets 
//
//val GROUPa = """([-0-9a-zA-Z.+_]+).accessrate_([-0-9a-zA-Z.+_]+)""".r
//val GROUPe = """([-0-9a-zA-Z.+_]+).editrate_([-0-9a-zA-Z.+_]+)""".r
val GROUPa = """(.+).accessrate_(.+)""".r
val GROUPe = """(.+).editrate_(.+)""".r


val k1: String = "2_A.L.accessrate_en___AVUS"
//val k2: String = "2_A.L.editrate_en___AVUS"
var k2: String = "dissertation_DEMO_2_A.L.editrate_en___First Racing"
var k3: String = "dissertation_DEMO_2_A.L.editrate_en___(First, Racing)"

val expr = "[ (),]".r   
k2 = expr.replaceAllIn(k2, "_") 

//
// Pattern matching fails if " " is in the key, so we replace it.
//
def cleanKey2 ( k:String ) : String = { expr.replaceAllIn(k, "_") } 
def cleanKey ( k:String ) : String = { URLEncoder.encode(k) } 

println( "Clean Key  : " + cleanKey( k2 ) )
println( "Clean Key  : " + cleanKey( k3 ) )

println( "Clean Key 2: " + cleanKey2( k2 ) )
println( "Clean Key 2: " + cleanKey2( k3 ) )


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

"2_A.L.accessrate_en___AVUS" match {
    case GROUPa(gr, page) =>
       println(gr)
       println(page)
}
	
getPageACCESS( k1 )
getGroupACCESS( k1 )

getPageEDITS( k2 )
getGroupEDITS( k2 )

// More examples: 
//   
// https://altiscale.zendesk.com/hc/en-us/articles/202627136-Spark-Shell-Examples
// 

val nameEr = "file:///mnt/hgfs/SHARE.VM.MAC/DATA/"+groupname+".editrate.tsb.seq"
val tsbEditsRDD = sc.sequenceFile(nameEr,classOf[Text], classOf[VectorWritable])

val nameAr = "file:///mnt/hgfs/SHARE.VM.MAC/DATA/"+groupname+".accessrate.tsb.seq"
val tsbAccessRDD = sc.sequenceFile(nameAr,classOf[Text], classOf[VectorWritable])

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

// ets.take(1)
// ats.take(1)

both.take(1)

// both.saveAsTextFile( "file:///home/training/training_materials/out/" + groupname + ".a_e_sum.csv" )

// Load and parse the data
// val resultFN = "file:///home/training/training_materials/out/" + groupname + ".a_e_sum.csv" 
// val result = sc.textFile(resultFN)
// result.take(1)

//
// Array[String] = Array((en___Vietnam,(605.0,3756903.0)))
//

import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors

val parsedData = both.map { 
  // x => ( x._1,( x._2._1,x._2._2) )
  rec => LabeledPoint( 0.0,  Vectors.dense( rec._2._1, rec._2._2 ) )
}

val finalResult  = both.map { 
  // x => ( x._1,( x._2._1,x._2._2) )
  rec => ( rec._2._1, rec._2._2 )
}

val resultFN = "file:///home/training/training_materials/out/" + groupname + ".a_e_sum.csv" 
finalResult.saveAsTextFile( "file:///home/training/training_materials/out/2col." + groupname + ".a_e_sum.csv" )



// Building the model
//val numIterations = 100
//val model = LinearRegressionWithSGD.train(parsedData, numIterations)

// Evaluate model on training examples and compute training error
//val valuesAndPreds = parsedData.map { point =>
//  val prediction = model.predict(point.features)
//  (point.label, prediction)
//}
//val MSE = valuesAndPreds.map{case(v, p) => math.pow((v - p), 2)}.mean()
//println("training Mean Squared Error = " + MSE)

 

