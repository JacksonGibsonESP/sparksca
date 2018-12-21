// --------Ссылки---------

//  https://data-flair.training/blogs/spark-mllib-data-types/
//  https://medium.com/@rickynguyen/getting-started-with-spark-day-5-36b62a6d13bf
//  http://www.cse.cuhk.edu.hk/~ericlo/teaching/bigdata/lab/5-MLlib/mllib-datatypes.html
//  https://spark.apache.org/docs/2.2.0/mllib-data-types.html





// ----------------------------Разряженный и плотный вектор------------------------------------

import org.apache.spark.mllib.linalg.{Vector, Vectors}

// Create a dense vector (1.0, 0.0, 3.0).
val dv: Vector = Vectors.dense(1.0, 0.0, 3.0)
// Create a sparse vector (1.0, 0.0, 3.0) by specifying its indices and values corresponding to nonzero entries.
val sv1: Vector = Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0))