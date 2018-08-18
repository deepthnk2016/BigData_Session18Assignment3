import spark.implicits._

/* Upload the dataset file */
val dataset_holiday=spark.read.csv("/home/bigdata/deepak/docs/Acadgild/Session18Assignment3/Dataset_Holidays.txt")
/*************************************************/

/*************************************************/
/* Problem Statement 3 - Start                   */
/* Which user has travelled largest distance till date */

println("###############################################################################")
println("      Problem 1 - Which age range <20, 20-35, >35 spends most in travel        ")
println("###############################################################################")

/* Get the sum of distance per year of per traveller */
val sumDistanceByUserMode=dataset_holiday.groupBy("_c0","_c3").agg(sum("_c4")).sort("_c0","_c3").toDF("user","travelmode","distance")

/* Read the user details input file to get the age of each user */
val dataset_users=spark.read.csv("/home/bigdata/deepak/docs/Acadgild/Session18Assignment3/Dataset_User_details.txt").toDF("user","name","age")

/* Now, join the above two dataframes to include age in the output */
val groupTotDistanceByAge=sumDistanceByUserMode.join(dataset_users,"user").select("age","travelmode","distance")

/* Read the details of cost per unit from dataset_transport.txt */
val dataset_transport=spark.read.csv("/home/bigdata/deepak/docs/Acadgild/Session18Assignment3/Dataset_Transport.txt").toDF("travelmode","priceperunit")

/* Now, join the above dataframe with groupTotDistanceByAge to get the total cost */
val groupCostByTravelmode=groupTotDistanceByAge.join(dataset_transport,"travelmode").toDF("travelmode","age","distance","priceperunit")

val getTotCostByAge=groupCostByTravelmode.withColumn("totalcost",$"distance"*$"priceperunit").toDF("travelmode","age","distance","priceperunit","totalcost")

val getTotCostByAge=groupCostByTravelmode.withColumn("totalcost",$"distance"*$"priceperunit").toDF("travelmode","age","distance","priceperunit","totalcost")

/* Now, add another column to display the age range */
val groupTotCostByAgeRange=getTotCostByAge.withColumn("agerange",when($"age" < 20,"<20").otherwise(when($"age">=20 and $"age"<=35,"20-35").otherwise(when ($"age">35,">35") )))

/* Get age range with max total cost*/
val getTotCostMaxAgeRange=groupTotCostByAgeRange.groupBy("agerange").agg(sum("totalcost")).toDF("agerange","totalcost").sort(desc("totalcost")).show(1)


