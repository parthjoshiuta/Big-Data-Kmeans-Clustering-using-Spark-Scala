import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.lang.Math._;

object KMeans {

  

  var centroids: Array[Point] = Array[Point]()
  var nc: Array[Point] = Array[Point]()
  var data : Array[Point] = Array[Point]()
  var c : Array[(Point, Point)] = Array[(Point, Point)]()
  type Point = (Double, Double);
  //Finding the nearest Centroid

  def find_centroid(c : Array[Point], p : Point) : Point = 
  {
  	
  	var min_dist : Double = 10000;
  	var eu_dist : Double = 10000;
  	var min_index : Int = 10000;
  	var count : Int = 0;

  	for(x <- c)
  	{
  		eu_dist = Math.sqrt(((x._1-p._1)*(x._1 - p._1)) + ((x._2-p._2)*(x._2-p._2)));
  		if(eu_dist < min_dist)
  		{
  			min_dist = eu_dist; 
    		min_index = count;
  		}
  		count = count + 1
  	}
    
  	return (c(min_index)._1, c(min_index)._2)
  }

  def reduce1(e : Array[(Point, Iterable[Point])]) : Array[Point] = 
  {
  	
  	var count : Int = 0;
  	var sx : Double = 0;
  	var sy : Double = 0;
    var ncentroids: Array[Point] = Array[Point]()
  	for(i <- 0 to (e.length-1))
  	{
      count = 0
      for(j <- e(i)._2)
      {
  		count= count+1 ;  
    	sx += j._1 ;
    	sy += j._2 ;
    }
    ncentroids :+= (sx/count, sy/count)
  	}
  	return ncentroids


  }


  def main(args: Array[ String ]) {
    /* ... */
    val conf = new SparkConf().setAppName("Kmeans");
    conf.setMaster("local[2]")
	  val sc = new SparkContext(conf);
    
/* read initial centroids from centroids.txt */
    centroids = sc.textFile(args(1)).collect.map( line => { val a = line.split(",")
                                                    (a(0).toDouble,a(1).toDouble)})
    data  = sc.textFile(args(0)).collect.map( x => {val a = x.split(",") 
                                              (a(0).toDouble, a(1).toDouble)});
 /*find new centroids using KMeans */
    for ( i <- 1 to 5 )
    {
    
    c = data.map(d => {((find_centroid(centroids, d), (d._1, d._2)))});
    var d = sc.parallelize(c)
    var e = d.mapValues(value => (value,1))
    var new_centroids = e.reduceByKey{ case ((x1, x2), (y1, y2)) => (((x1._1 + y1._1),(x1._2 + y1._2)), (x2 + y2)) }             
                 .mapValues{
                  case (x, y) => (x._1/y, x._2/y) }
                 .collect
    centroids = new_centroids.map(a => (a._2))
    }
    
    centroids.foreach(println)
  }
}
