import org.apache.spark.sql.SparkSession

object beverages {
  def main(args: Array[String]): Unit = {
    val test = SparkSession.builder.master("local").appName("scala").enableHiveSupport().getOrCreate()
     // Create database
    //test.sql("create database proj1").show()

    test.sql("show databases").show()
    test.sql("use proj1").show()
    // test.sql("create table cb1 (coff string, branch string) row format delimited fields terminated by ',' stored as textfile")
    //test.sql("load data local inpath 'dataset1/allbranch.txt' overwrite into table cb1")
    // test.sql("create table cb2 (coff2 string, customer int) row format delimited fields terminated by ',' stored as textfile").show()
    // test.sql("load data local inpath 'dataset1/allcustomer.txt' overwrite into table cb2")

    //SCENARIO 1
    //table
    //test.sql("create view branch_1 as select cb1.coff, sum(cb2.customer)as total from cb1 inner join cb2 on cb1.coff = cb2.coff2 and branch = 'Branch1' group by coff")//test.sql("select * from branch_1").show(100,20,false)
    //*****
    //test.sql("select sum(total) as total_customer from branch_1").show()
    //test.sql("create view branch_2 as select cb1.coff, sum(cb2.customer) as total from cb1 inner join cb2 on cb1.coff = cb2.coff2 and branch = 'Branch2' group by coff")
    //test.sql("select * from branch_2").show(60,20,false)
    //*****
    //test.sql("select sum(total) as total_customer from branch_2").show()

    //SCENARIO 2
    //*****
    //test.sql("select max(total) as most_consumed from branch_1").show()
    //*****
    //test.sql("select max(total) as most_consumed from branch_2").show()
    //*****
    //test.sql("select percentile(cast(total as BIGINT) , 0.5) from branch_2").show()
    //*****
    //test.sql("select avg(total) as average from branch_2").show()
    // test.sql("select min(total) as least_consumed from branch_2").show()

    //SCENARIO 3
    //*****
    //test.sql("select distinct coff as availabe_beverages  from cb1 where branch in ( 'Branch10','Branch8' , 'Branch1')").show(60,0,false)
    //test.sql("create view common7 as select * from cb1 where branch = 'Branch7'")
    //test.sql("create view common4 as select * from cb1 where branch = 'Branch4'")
    //*****
    //test.sql("select common4.coff as common_beverages from common4 inner join common7 on common4.coff = common7.coff group by common4.coff").show(60,0,false)

    //SCENARIO 4
    //test.sql("create table beforepart (available_bev string, branch string) row format delimited fields terminated by ',' stored as textfile")
    //test.sql("load data local inpath 'dataset1/beforepart.txt' overwrite into table beforepart")
    //*****
    //test.sql("select * from beforepart").show(45, 0,false)
    //test.sql("create table afterpart(available_bev string) partitioned by (branch string)")
    //test.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    //test.sql("insert overwrite table afterpart partition(branch) select available_bev, branch from beforepart")
    //*****
    //test.sql("select * from afterpart").show(50,0,false)
    //test.sql("create view branch_beverages as select distinct coff as availabe_beverages  from cb1 where branch in ( 'Branch10','Branch8' , 'Branch1')")
    //*****
    //test.sql("select * from branch_beverages").show(50,0 ,false)

    //SCENARIO 5
    //test.sql("ALTER View common7 SET TBLPROPERTIES ( 'note' = \"this is  a view named common7 \")")
    //*****
    //test.sql("Show TBLPROPERTIES common7").show(100,50,false)
    //test.sql("ALTER View common7 SET TBLPROPERTIES ( 'comment' = \"the table contains beverages from branch 7 \")")
    //*****
    //test.sql("Show TBLPROPERTIES common7").show(300,200,false)

    //SCENARIO 6
    //test.sql("create table row_delete like branch_1")
    //test.sql("insert into row_delete select * from branch_1")
    //test.sql("insert overwrite table row_deleted select coff, total from (select *,  row_number () over () as row_num from row_deleted)as numbered_rows where row_num!=19")
    //*****
    //test.sql("select * from row_delete")

  }

}
