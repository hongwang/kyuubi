
package org.apache.kyuubi.plugin.lineage.helper

import org.apache.kyuubi.plugin.lineage.helper.SparkListenerHelper.SPARK_RUNTIME_VERSION
import org.apache.spark.SparkConf
import org.apache.spark.kyuubi.lineage.LineageConf
import org.apache.spark.kyuubi.lineage.SparkContextHelper
import org.apache.spark.sql.SparkListenerExtensionTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.kyuubi.KyuubiFunSuite

import scala.reflect.io.File


class SparkSQLColumnParseHelperSuite extends KyuubiFunSuite
  with SparkListenerExtensionTest {

  def catalogName: String = "org.apache.spark.sql.connector.catalog.InMemoryTableCatalog"

  val DEFAULT_CATALOG = LineageConf.DEFAULT_CATALOG
  override protected val catalogImpl: String = "hive"

  override def sparkConf(): SparkConf = {
    super.sparkConf()
      .set(
        "spark.sql.catalog.v2_catalog",
        catalogName)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql("create database if not exists test_db")
    spark.sql("create database if not exists test_db0")
    spark.sql("create table if not exists test_db0.test_table0" +
      " (key int, value string) using parquet")
    spark.sql("create table if not exists test_db0.test_table_part0" +
      " (key int, value string, pid string) using parquet" +
      "  partitioned by(pid)")
    spark.sql("create table if not exists test_db0.test_table1" +
      " (key int, value string) using parquet")
    spark.sql("create table test_db.test_table_from_dir" +
      " (`a0` string, `b0` string) using parquet")
  }

  override def afterAll(): Unit = {
    Seq(
      "test_db0.test_table0",
      "test_db0.test_table1",
      "test_db0.test_table_part0",
      "test_db.test_table_from_dir").foreach { t =>
      spark.sql(s"drop table if exists $t")
    }
    spark.sql("drop database if exists test_db")
    spark.sql("drop database if exists test_db0")
    spark.stop()
    super.afterAll()
  }

  test("columns extract - AlterViewAsCommand") {
    withView("alterviewascommand", "alterviewascommand1") { _ =>
      spark.sql("create view alterviewascommand as select key from test_db0.test_table0")
      val ret0 =
        extractColumns("alter view alterviewascommand as select key from test_db0.test_table0")
      assert(ret0 ==
        List(
          s"$DEFAULT_CATALOG.test_db0.test_table0.key"))

      spark.sql("create view alterviewascommand1 as select * from test_db0.test_table0")
      val ret1 =
        extractColumns("alter view alterviewascommand1 as select * from test_db0.test_table0")

      assert(ret1 ==
        List(
            s"$DEFAULT_CATALOG.test_db0.test_table0.key",
            s"$DEFAULT_CATALOG.test_db0.test_table0.value"))
    }
  }


  test("columns lineage extract - DataSourceV2Relation") {
    val ddls =
      """
        |create table v2_catalog.db.tbb(col1 string, col2 string, col3 string)
        |""".stripMargin

    ddls.split("\n").filter(_.nonEmpty).foreach(spark.sql(_).collect())
    withView("test_view") { _ =>
      val result = extractColumns(
        "create view test_view(a, b, c) as" +
          " select col1 as a, col2 as b, col3 as c from v2_catalog.db.tbb")
      assert(result ==
        List(
          "v2_catalog.db.tbb.col1",
          "v2_catalog.db.tbb.col2",
          "v2_catalog.db.tbb.col3"))
    }
  }


  test("columns lineage extract - AppendData/OverwriteByExpression") {
    val ddls =
      """
        |create table v2_catalog.db.tb0(col1 int, col2 string) partitioned by(col2)
        |""".stripMargin
    ddls.split("\n").filter(_.nonEmpty).foreach(spark.sql(_).collect())
    withTable("v2_catalog.db.tb0") { _ =>
      val ret0 =
        extractColumns(
          s"insert into table v2_catalog.db.tb0 " +
            s"select key as col1, value as col2 from test_db0.test_table0")
      assert(ret0 ==
        List(
          s"$DEFAULT_CATALOG.test_db0.test_table0.key",
          s"$DEFAULT_CATALOG.test_db0.test_table0.value"))

      val ret1 =
        extractColumns(
          s"insert overwrite table v2_catalog.db.tb0 partition(col2) " +
            s"select key as col1, value as col2 from test_db0.test_table0")
      assert(ret1 ==
        List(
          s"$DEFAULT_CATALOG.test_db0.test_table0.key",
          s"$DEFAULT_CATALOG.test_db0.test_table0.value"))

      val ret2 =
        extractColumns(
          s"insert overwrite table v2_catalog.db.tb0 partition(col2 = 'bb') " +
            s"select key as col1 from test_db0.test_table0")
      assert(ret2 ==
        List(s"$DEFAULT_CATALOG.test_db0.test_table0.key"))
    }
  }


  test("columns lineage extract - CreateViewCommand") {
    withView("createviewcommand", "createviewcommand1", "createviewcommand2") { _ =>
      val ret0 = extractColumns(
        "create view createviewcommand(a, b) as select key, value from test_db0.test_table0")
      assert(ret0 ==
        List(s"$DEFAULT_CATALOG.test_db0.test_table0.key",
          s"$DEFAULT_CATALOG.test_db0.test_table0.value"))

      val ret1 = extractColumns(
        "create view createviewcommand1 as select key, value from test_db0.test_table0")
      assert(ret1 ==
        List(
            s"$DEFAULT_CATALOG.test_db0.test_table0.key",
            s"$DEFAULT_CATALOG.test_db0.test_table0.value"))

      val ret2 = extractColumns(
        "create view createviewcommand2 as select * from test_db0.test_table0")
      assert(ret2 ==
        List(
            s"$DEFAULT_CATALOG.test_db0.test_table0.key",
            s"$DEFAULT_CATALOG.test_db0.test_table0.value"))
    }

  }

  test("columns lineage extract - CreateDataSourceTableAsSelectCommand") {
    withTable("createdatasourcetableasselectcommand", "createdatasourcetableasselectcommand1") {
      _ =>
        val ret0 =
          extractColumns("create table createdatasourcetableasselectcommand using parquet" +
            " AS SELECT key, value FROM test_db0.test_table0")
        assert(ret0 ==
          List(
            s"$DEFAULT_CATALOG.test_db0.test_table0.key",
            s"$DEFAULT_CATALOG.test_db0.test_table0.value"))

        val ret1 =
          extractColumns("create table createdatasourcetableasselectcommand1 using parquet" +
            " AS SELECT * FROM test_db0.test_table0")
        assert(ret1 ==
          List(
            s"$DEFAULT_CATALOG.test_db0.test_table0.key",
            s"$DEFAULT_CATALOG.test_db0.test_table0.value"))
    }
  }

  test("columns lineage extract - CreateHiveTableAsSelectCommand") {
    withTable("createhivetableasselectcommand", "createhivetableasselectcommand1") { _ =>
      val ret0 = extractColumns("create table createhivetableasselectcommand using hive" +
        " as select key, value from test_db0.test_table0")
      assert(ret0 ==
        List(
          s"$DEFAULT_CATALOG.test_db0.test_table0.key",
          s"$DEFAULT_CATALOG.test_db0.test_table0.value"))

      val ret1 = extractColumns("create table createhivetableasselectcommand1 using hive" +
        " as select * from test_db0.test_table0")
      assert(ret1 ==
        List(
          s"$DEFAULT_CATALOG.test_db0.test_table0.key",
          s"$DEFAULT_CATALOG.test_db0.test_table0.value"))
    }
  }

  test("columns lineage extract - OptimizedCreateHiveTableAsSelectCommand") {
    withTable("optimizedcreatehivetableasselectcommand") { _ =>
      val ret =
        extractColumns(
          "create table optimizedcreatehivetableasselectcommand stored as parquet " +
            "as select * from test_db0.test_table0")
      assert(ret ==
        List(
          s"$DEFAULT_CATALOG.test_db0.test_table0.key",
          s"$DEFAULT_CATALOG.test_db0.test_table0.value"))
    }
  }

  test("columns lineage extract - CreateTableAsSelect") {
    withTable(
      "v2_catalog.db.createhivetableasselectcommand",
      "v2_catalog.db.createhivetableasselectcommand1") { _ =>
      val ret0 = extractColumns("create table v2_catalog.db.createhivetableasselectcommand" +
        " as select key, value from test_db0.test_table0")
      assert(ret0 ==
        List(
          s"$DEFAULT_CATALOG.test_db0.test_table0.key",
          s"$DEFAULT_CATALOG.test_db0.test_table0.value"))

      val ret1 = extractColumns("create table v2_catalog.db.createhivetableasselectcommand1" +
        " as select * from test_db0.test_table0")
      assert(ret1 ==
        List(
          s"$DEFAULT_CATALOG.test_db0.test_table0.key",
          s"$DEFAULT_CATALOG.test_db0.test_table0.value"))
    }
  }

  test("columns lineage extract - InsertIntoDataSourceCommand") {
    val tableName = "insertintodatasourcecommand"
    withTable(tableName) { _ =>
      val schema = new StructType()
        .add("a", IntegerType, nullable = true)
        .add("b", StringType, nullable = true)
      val newTable = CatalogTable(
        identifier = TableIdentifier(tableName, None),
        tableType = CatalogTableType.MANAGED,
        storage = CatalogStorageFormat(
          locationUri = None,
          inputFormat = None,
          outputFormat = None,
          serde = None,
          compressed = false,
          properties = Map.empty),
        schema = schema,
        provider = Some(classOf[SimpleInsertSource].getName))
      spark.sessionState.catalog.createTable(newTable, ignoreIfExists = false)

      val ret0 =
        extractColumns(
          s"insert into table  $tableName select key, value from test_db0.test_table0")
      assert(ret0 ==
        List(
          s"$DEFAULT_CATALOG.test_db0.test_table0.key",
          s"$DEFAULT_CATALOG.test_db0.test_table0.value"))

      val ret1 =
        extractColumns(
          s"insert into table  $tableName select * from test_db0.test_table0")
      assert(ret1 ==
        List(
          s"$DEFAULT_CATALOG.test_db0.test_table0.key",
          s"$DEFAULT_CATALOG.test_db0.test_table0.value"))

      val ret2 =
        extractColumns(
          s"insert into table  $tableName " +
            s"select (select key from test_db0.test_table1 limit 1) + 1 as aa, " +
            s"value as bb from test_db0.test_table0")
      assert(ret2 ==
        List(
          s"$DEFAULT_CATALOG.test_db0.test_table1.key",
          s"$DEFAULT_CATALOG.test_db0.test_table0.value"))

    }
  }

  test("columns lineage extract - InsertIntoHadoopFsRelationCommand") {
    val tableName = "insertintohadoopfsrelationcommand"
    withTable(tableName) { _ =>
      spark.sql(s"CREATE TABLE $tableName (a int, b string) USING parquet")
      val ret0 =
        extractColumns(
          s"insert into table $tableName select key, value from test_db0.test_table0")

      assert(ret0 ==
        List(
          s"$DEFAULT_CATALOG.test_db0.test_table0.key",
          s"$DEFAULT_CATALOG.test_db0.test_table0.value"))
    }

  }

  test("columns lineage extract - InsertIntoDatasourceDirCommand") {
    val tableDirectory = getClass.getResource("/").getPath + "table_directory"
    val directory = File(tableDirectory).createDirectory()
    val ret0 = extractColumns(s"""
                                 |INSERT OVERWRITE DIRECTORY '${directory.path}'
                                 |USING parquet
                                 |SELECT * FROM test_db0.test_table_part0""".stripMargin)
    assert(ret0 ==
      List(
        s"$DEFAULT_CATALOG.test_db0.test_table_part0.key",
        s"$DEFAULT_CATALOG.test_db0.test_table_part0.value",
        s"$DEFAULT_CATALOG.test_db0.test_table_part0.pid"))
  }

  test("columns lineage extract - InsertIntoHiveDirCommand") {
    val tableDirectory = getClass.getResource("/").getPath + "table_directory"
    val directory = File(tableDirectory).createDirectory()
    val ret0 = extractColumns(s"""
                                 |INSERT OVERWRITE DIRECTORY '${directory.path}'
                                 |USING parquet
                                 |SELECT * FROM test_db0.test_table_part0""".stripMargin)
    assert(ret0 ==
      List(
        s"$DEFAULT_CATALOG.test_db0.test_table_part0.key",
        s"$DEFAULT_CATALOG.test_db0.test_table_part0.value",
        s"$DEFAULT_CATALOG.test_db0.test_table_part0.pid"))
  }

  test("columns lineage extract - InsertIntoHiveTable") {
    val tableName = "insertintohivetable"
    withTable(tableName) { _ =>
      spark.sql(s"CREATE TABLE $tableName (a int, b string) USING hive")
      val ret0 =
        extractColumns(
          s"insert into table $tableName select * from test_db0.test_table0")

      assert(ret0 ==
        List(
          s"$DEFAULT_CATALOG.test_db0.test_table0.key",
          s"$DEFAULT_CATALOG.test_db0.test_table0.value"))
    }

  }

  test("columns lineage extract - logical relation sql") {
    val ret0 = extractColumns("select key, value from test_db0.test_table0")
    assert(ret0 ==
      List(
        s"$DEFAULT_CATALOG.test_db0.test_table0.key",
        s"$DEFAULT_CATALOG.test_db0.test_table0.value"))

    val ret1 = extractColumns("select * from test_db0.test_table_part0")
    assert(ret1 ==
      List(
        s"$DEFAULT_CATALOG.test_db0.test_table_part0.key",
        s"$DEFAULT_CATALOG.test_db0.test_table_part0.value",
        s"$DEFAULT_CATALOG.test_db0.test_table_part0.pid"))

  }

  test("columns lineage extract - not generate lineage sql") {
    val ret0 = extractColumns("create table test_table1(a string, b string, c string)")
    assert(ret0 == List[String]())
  }

  test("columns lineage extract - data source V2 sql") {
    val ddls =
      """
        |create table v2_catalog.db.tb(col1 string, col2 string, col3 string)
        |""".stripMargin

    ddls.split("\n").filter(_.nonEmpty).foreach(spark.sql(_).collect())
    withTable("v2_catalog.db.tb") { _ =>
      val sql0 = "select col1 from v2_catalog.db.tb"
      val ret0 = extractColumns(sql0)
      assert(ret0 ==
        List("v2_catalog.db.tb.col1"))

      val sql1 = "select col1, hash(hash(col1)) as col2 from v2_catalog.db.tb"
      val ret1 = extractColumns(sql1)
      assert(ret1 ==
        List("v2_catalog.db.tb.col1"))

      val sql2 =
        "select col1, case col1 when '1' then 's1' else col1 end col2 from v2_catalog.db.tb"
      val ret2 = extractColumns(sql2)
      assert(ret2 ==
        List("v2_catalog.db.tb.col1"))

      val sql3 =
        "select col1 as col2, 'col2' as col2, 'col2', first(col3) as col2 " +
          "from v2_catalog.db.tb group by col1"
      val ret3 = extractColumns(sql3)
      assert(ret3 ==
        List(
          "v2_catalog.db.tb.col1",
          "v2_catalog.db.tb.col3"))

      val sql4 =
        "select col1 as col2, sum(hash(col1) + hash(hash(col1))) " +
          "from v2_catalog.db.tb group by col1"
      val ret4 = extractColumns(sql4)
      assert(ret4 ==
        List(
          "v2_catalog.db.tb.col1"))
      val sql5 =
        s"""
           | select t1.col2, count(t2.col3)
           | from
           | (select col1 as col2 from v2_catalog.db.tb) t1 join
           | (select col1 as col3 from v2_catalog.db.tb) t2
           |  on t1.col2 = t2.col3
           |  group by 1
           |""".stripMargin
      val ret5 = extractColumns(sql5)
      assert(ret5 ==
        List(
          "v2_catalog.db.tb.col1"))
    }
  }

  test("columns lineage extract - base sql") {
    val ddls =
      List(
        "CREATE TABLE tmp0 AS SELECT * FROM VALUES(1),(2),(3) AS t(tmp0_0)",
        "CREATE TABLE tmp1 AS select c1 as tmp1_0, c2 as tmp1_1,  c3 as tmp1_2," +
          "concat(c1, c2) as tmp1_3 from" +
          " VALUES(1,'a',4),(2, 'b', 4),(3, 'c', 4) AS t(c1, c2, c3)")

    ddls.foreach(spark.sql(_).collect())

    withTable("tmp0", "tmp1") { _ =>
      val sql0 =
        """
          |select tmp0_0 as a0, tmp1_0 as a1 from tmp0 join tmp1 where tmp1_0 = tmp0_0
          |""".stripMargin
      val sql0ExpectResult =
        List(
          s"$DEFAULT_CATALOG.default.tmp0.tmp0_0",
          s"$DEFAULT_CATALOG.default.tmp1.tmp1_0")

      val sql1 =
        """
          |select count(tmp1_0) as cnt, tmp1_1 from tmp1 group by tmp1_1
          |""".stripMargin
      val sql1ExpectResult =
        List(
          s"$DEFAULT_CATALOG.default.tmp1.tmp1_0",
          s"$DEFAULT_CATALOG.default.tmp1.tmp1_1")

      val ret0 = extractColumns(sql0)
      assert(ret0 == sql0ExpectResult)
      val ret1 = extractColumns(sql1)
      assert(ret1 == sql1ExpectResult)
    }
  }

  test("columns lineage extract - CTE sql") {
    val ddls =
      List(
        "create table test_db.goods_detail0(goods_id string, cat_id string)",
        "create table v2_catalog.test_db_v2.goods_detail1" +
          "(goods_id string, cat_id string, product_id string)",
        "create table v2_catalog.test_db_v2.mall_icon_schedule" +
          "(relation_id string, icon_id string," +
          " icon_type string, is_enabled string, start_time date, end_time date)",
        "create table v2_catalog.test_db_v2.mall_icon" +
          "(id string, name string, is_enabled string, start_time date, end_time date)")
    ddls.foreach(spark.sql(_).collect())
    withTable(
      "test_db.goods_detail0",
      "v2_catalog.test_db_v2.goods_detail1",
      "v2_catalog.test_db_v2.mall_icon_schedule",
      "v2_catalog.test_db_v2.mall_icon") { _ =>
      val sql0 =
        """WITH base_goods_detail AS (
          |SELECT goods_id
          |, CASE
          |WHEN cat_id = 1 THEN 'car'
          |ELSE 'other'
          |END AS cate_grory
          |FROM test_db.goods_detail0
          |GROUP BY goods_id, CASE
          |WHEN cat_id = 1 THEN 'car'
          |ELSE 'other'
          |END
          |),
          |goods_cat AS (
          |SELECT t1.goods_id, t1.cat_id, t1.product_id, t2.start_time, t2.end_time
          |FROM v2_catalog.test_db_v2.goods_detail1 t1
          |JOIN (
          |SELECT t1.relation_id, t1.start_time AS start_time,
          |t2.end_time AS end_time, t2.id, t2.is_enabled
          |FROM  v2_catalog.test_db_v2.mall_icon_schedule t1
          |JOIN v2_catalog.test_db_v2.mall_icon t2 ON t1.icon_id = t2.id
          |WHERE t2.name LIKE '%test%'
          |AND t2.is_enabled = 'Y'
          |AND t1.icon_type = 'short'
          |AND t1.is_enabled = 'Y'
          |) t2
          |ON t1.product_id = t2.relation_id
          |),
          |goods_cat_new AS (
          |SELECT t1.goods_id, t1.cate_grory, t2.cat_id, t2.product_id, t2.start_time
          |, t2.end_time
          |FROM base_goods_detail t1
          |JOIN goods_cat t2 ON t1.goods_id = t2.goods_id
          |)
          |SELECT *
          |FROM goods_cat_new
          |LIMIT 10""".stripMargin

      val ret0 = extractColumns(sql0)
      assert(ret0 ==
        List(
          s"$DEFAULT_CATALOG.test_db.goods_detail0.goods_id",
          s"$DEFAULT_CATALOG.test_db.goods_detail0.cat_id",
          "v2_catalog.test_db_v2.goods_detail1.cat_id",
          "v2_catalog.test_db_v2.goods_detail1.product_id",
          "v2_catalog.test_db_v2.goods_detail1.goods_id",
          "v2_catalog.test_db_v2.mall_icon_schedule.start_time",
          "v2_catalog.test_db_v2.mall_icon_schedule.relation_id",
          "v2_catalog.test_db_v2.mall_icon_schedule.icon_id",
          "v2_catalog.test_db_v2.mall_icon_schedule.icon_type",
          "v2_catalog.test_db_v2.mall_icon_schedule.is_enabled",
          "v2_catalog.test_db_v2.mall_icon.end_time",
          "v2_catalog.test_db_v2.mall_icon.id",
          "v2_catalog.test_db_v2.mall_icon.name",
          "v2_catalog.test_db_v2.mall_icon.is_enabled"))
    }
  }

  test("columns lineage extract - join sql ") {
    val ddls =
      """
        |create table v2_catalog.db.tb1(col1 string, col2 string, col3 string)
        |create table v2_catalog.db.tb2(col1 string, col2 string, col3 string)
        |""".stripMargin
    ddls.split("\n").filter(_.nonEmpty).foreach(spark.sql(_).collect())
    withTable("v2_catalog.db.tb1", "v2_catalog.db.tb2") { _ =>
      val sql0 =
        """
          |select t1.col1 as a1, t2.col1 as b1, concat(t1.col1, t2.col1) as ab1,
          |concat(concat(t1.col1, t2.col2), concat(t1.col2, t2.col3)) as ab2
          |from v2_catalog.db.tb1 t1 join v2_catalog.db.tb2 t2
          |on t1.col1 = t2.col1
          |""".stripMargin

      val ret0 = extractColumns(sql0)
      assert(
        ret0 ==
          List(
            "v2_catalog.db.tb1.col1",
            "v2_catalog.db.tb1.col2",
            "v2_catalog.db.tb2.col1",
            "v2_catalog.db.tb2.col2",
            "v2_catalog.db.tb2.col3"))
    }
  }

  test("columns lineage extract - union sql") {
    val ddls =
      """
        |create table test_db.test_table0(a int, b string, c string)
        |create table test_db.test_table1(a int, b string, c string)
        |""".stripMargin
    ddls.split("\n").filter(_.nonEmpty).foreach(spark.sql(_).collect())
    withTable("test_db.test_table0", "test_db.test_table1") { _ =>
      val sql0 =
        """
          |select a, b, c from (
          |select a, b, b as c from test_db.test_table0
          |union
          |select a, b, c as c from test_db.test_table1
          |) a
          |""".stripMargin
      val ret0 = extractColumns(sql0)
      assert(ret0 ==
        List(
              s"$DEFAULT_CATALOG.test_db.test_table0.a",
          s"$DEFAULT_CATALOG.test_db.test_table0.b",
              s"$DEFAULT_CATALOG.test_db.test_table1.a",
              s"$DEFAULT_CATALOG.test_db.test_table1.b",
              s"$DEFAULT_CATALOG.test_db.test_table1.c"))
    }
  }

  test("columns lineage extract - agg and union sql") {
    val ddls =
      List(
        "create table test_db.test_order_item(stat_date string, pay_time date," +
          "channel_id string, sub_channel_id string," +
          "user_type string, country_name string," +
          "order_id string, goods_count int, shop_price int, is_valid_order int)",
        "create table test_db.test_p0_order_item(pay_time date," +
          "channel_id string, sub_channel_id string," +
          "user_type string, country_name string, is_valid_item int)")
    ddls.foreach(spark.sql(_).collect())
    withTable("test_db.test_order_item", "test_db.test_p0_order_item") { _ =>
      val sql0 =
        """
          |SELECT stat_date, channel_id, sub_channel_id
          |, user_type, country_name, SUM(get_count) AS get_count0
          |, SUM(get_amount) as get_amount0,
          |cast(unix_timestamp() as bigint) AS add_time
          |FROM (
          |SELECT stat_date, channel_id, sub_channel_id, user_type
          |,country_name, COUNT(DISTINCT order_id) AS get_count
          |, SUM(goods_count * shop_price) AS get_amount
          |FROM test_db.test_order_item
          |WHERE 1 = 1 AND is_valid_order = 1
          |GROUP BY
          |stat_date, channel_id, sub_channel_id, user_type, country_name
          |) a
          |GROUP BY
          |stat_date, channel_id, sub_channel_id, user_type, country_name
          |""".stripMargin
      val ret0 = extractColumns(sql0)
      assert(ret0 ==
        List(
          s"$DEFAULT_CATALOG.test_db.test_order_item.stat_date",
          s"$DEFAULT_CATALOG.test_db.test_order_item.channel_id",
          s"$DEFAULT_CATALOG.test_db.test_order_item.sub_channel_id",
          s"$DEFAULT_CATALOG.test_db.test_order_item.user_type",
          s"$DEFAULT_CATALOG.test_db.test_order_item.country_name",
          s"$DEFAULT_CATALOG.test_db.test_order_item.order_id",
          s"$DEFAULT_CATALOG.test_db.test_order_item.goods_count",
          s"$DEFAULT_CATALOG.test_db.test_order_item.shop_price",
          s"$DEFAULT_CATALOG.test_db.test_order_item.is_valid_order"
          ))
      val sql1 =
        """
          |SELECT channel_id, sub_channel_id, country_name, SUM(get_count) AS get_count0
          |, SUM(get_amount) as get_amount0, cast(unix_timestamp() as bigint) AS add_time
          |FROM (
          |SELECT
          |channel_id, sub_channel_id, country_name, COUNT(DISTINCT order_id) AS get_count,
          |SUM(goods_count * shop_price) AS get_amount
          |FROM test_db.test_order_item
          |WHERE 1 = 1 AND is_valid_order = 1
          |GROUP BY
          |channel_id, sub_channel_id, country_name
          |UNION ALL
          |SELECT
          |channel_id, sub_channel_id, country_name, 0 AS get_count, 0 AS get_amount
          |FROM test_db.test_p0_order_item
          |WHERE 1 = 1
          |AND is_valid_item = 1
          |GROUP BY
          |channel_id, sub_channel_id, country_name
          |) a
          |GROUP BY a.channel_id, a.sub_channel_id, a.country_name
          |""".stripMargin
      val ret1 = extractColumns(sql1)
      assert(ret1 ==
        List(
          s"$DEFAULT_CATALOG.test_db.test_order_item.channel_id",
          s"$DEFAULT_CATALOG.test_db.test_order_item.sub_channel_id",
          s"$DEFAULT_CATALOG.test_db.test_order_item.country_name",
          s"$DEFAULT_CATALOG.test_db.test_order_item.order_id",
          s"$DEFAULT_CATALOG.test_db.test_order_item.goods_count",
          s"$DEFAULT_CATALOG.test_db.test_order_item.shop_price",
          s"$DEFAULT_CATALOG.test_db.test_order_item.is_valid_order",
          s"$DEFAULT_CATALOG.test_db.test_p0_order_item.channel_id",
          s"$DEFAULT_CATALOG.test_db.test_p0_order_item.sub_channel_id",
          s"$DEFAULT_CATALOG.test_db.test_p0_order_item.country_name",
          s"$DEFAULT_CATALOG.test_db.test_p0_order_item.is_valid_item"
          ))
    }
  }

  test("columns lineage extract - agg sql") {
    val sql0 = """select key as a, count(*) as b, 1 as c from test_db0.test_table0 group by key"""
    val ret0 = extractColumns(sql0)
    assert(ret0 ==
      List(
        s"$DEFAULT_CATALOG.test_db0.test_table0.key",
        ))

    val sql1 = """select count(*) as a, 1 as b from test_db0.test_table0"""
    val ret1 = extractColumns(sql1)
    assert(ret1 ==
      List())

    val sql2 = """select every(key == 1) as a, 1 as b from test_db0.test_table0"""
    val ret2 = extractColumns(sql2)
    assert(ret2 ==
      List(
        s"$DEFAULT_CATALOG.test_db0.test_table0.key"))

    val sql3 = """select count(*) as a, 1 as b from test_db0.test_table0"""
    val ret3 = extractColumns(sql3)
    assert(ret3 ==
      List())

    val sql4 = """select first(key) as a, 1 as b from test_db0.test_table0"""
    val ret4 = extractColumns(sql4)
    assert(ret4 ==
      List(
        s"$DEFAULT_CATALOG.test_db0.test_table0.key"))

    val sql5 = """select avg(key) as a, 1 as b from test_db0.test_table0"""
    val ret5 = extractColumns(sql5)
    assert(ret5 ==
      List(
        s"$DEFAULT_CATALOG.test_db0.test_table0.key"))

    val sql6 =
      """select count(value) + sum(key) as a,
        | 1 as b from test_db0.test_table0""".stripMargin
    val ret6 = extractColumns(sql6)
    assert(ret6 ==
      List(
            s"$DEFAULT_CATALOG.test_db0.test_table0.value",
            s"$DEFAULT_CATALOG.test_db0.test_table0.key"))

    val sql7 = """select count(*) + sum(key) as a, 1 as b from test_db0.test_table0"""
    val ret7 = extractColumns(sql7)
    assert(ret7 ==
      List(
        s"$DEFAULT_CATALOG.test_db0.test_table0.key"))

  }

  test("colums lineage extract - catch table") {
    val ddls =
      """
        |create table table0(a int, b string, c string)
        |create table table1(a int, b string, c string)
        |""".stripMargin
    ddls.split("\n").filter(_.nonEmpty).foreach(spark.sql(_).collect())
    withTable("table0", "table1") { _ =>
      spark.sql("cache table t0_cached select a as a0, b as b0 from table0 where a = 1 ")
      val sql0 =
        """
          |select b.a as aa, t0_cached.b0 as bb from t0_cached join table1 b on b.a = t0_cached.a0
          |""".stripMargin
      val ret0 = extractColumns(sql0)
      assert(ret0 ==
        List(
          s"$DEFAULT_CATALOG.default.table0.a",
          s"$DEFAULT_CATALOG.default.table0.b",
          s"$DEFAULT_CATALOG.default.table1.a"))

      val df0 = spark.sql("select a as a0, b as b0 from table0 where a = 2")
      df0.cache()
      val df1 = spark.sql("select a, b from table1")
      val df = df0.join(df1).select(df0("a0").alias("aa"), df1("b").alias("bb"))
      val analyzed = df.queryExecution.analyzed
      val ret1 = SparkSQLColumnParseHelper(spark).extractColumn(0, analyzed).get
      assert(ret1 ==
        List(
          s"$DEFAULT_CATALOG.default.table0.a",
          s"$DEFAULT_CATALOG.default.table0.b",
          s"$DEFAULT_CATALOG.default.table1.b",
          s"$DEFAULT_CATALOG.default.table1.a"))
    }
  }

  test("columns lineage extract - subquery sql") {
    val ddls =
      """
        |create table table0(a int, b string, c string)
        |create table table1(a int, b string, c string)
        |""".stripMargin
    ddls.split("\n").filter(_.nonEmpty).foreach(spark.sql(_).collect())
    withTable("table0", "table1") { _ =>
      val sql0 =
        """
          |select a as aa, bb, cc from (select b as bb, c as cc from table1) t0, table0
          |""".stripMargin
      val ret0 = extractColumns(sql0)
      assert(ret0 ==
        List(
          s"$DEFAULT_CATALOG.default.table1.b",
          s"$DEFAULT_CATALOG.default.table1.c",
          s"$DEFAULT_CATALOG.default.table0.a"))

      val sql1 =
        """
          |select (select a from table1) as aa, b as bb from table1
          |""".stripMargin
      val ret1 = extractColumns(sql1)
      assert(ret1 ==
        List(
          s"$DEFAULT_CATALOG.default.table1.a",
          s"$DEFAULT_CATALOG.default.table1.b"))

      val sql2 =
        """
          |select (select count(*) from table0) as aa, b as bb from table1
          |""".stripMargin
      val ret2 = extractColumns(sql2)
      assert(ret2 ==
        List(
          s"$DEFAULT_CATALOG.default.table1.b"))

      // ListQuery
      val sql3 =
        """
          |select * from table0 where table0.a in (select a from table1)
          |""".stripMargin
      val ret3 = extractColumns(sql3)
      assert(ret3 ==
        List(
          s"$DEFAULT_CATALOG.default.table0.a",
          s"$DEFAULT_CATALOG.default.table0.b",
          s"$DEFAULT_CATALOG.default.table0.c"))

      // Exists
      val sql4 =
        """
          |select * from table0 where exists (select * from table1 where table0.c = table1.c)
          |""".stripMargin
      val ret4 = extractColumns(sql4)
      assert(ret4 ==
        List(
          s"$DEFAULT_CATALOG.default.table0.a",
          s"$DEFAULT_CATALOG.default.table0.b",
          s"$DEFAULT_CATALOG.default.table0.c"))

      val sql5 =
        """
          |select * from table0 where exists (select * from table1 where c = "odone")
          |""".stripMargin
      val ret5 = extractColumns(sql5)
      assert(ret5 ==
        List(
          s"$DEFAULT_CATALOG.default.table0.a",
          s"$DEFAULT_CATALOG.default.table0.b",
          s"$DEFAULT_CATALOG.default.table0.c"))

      val sql6 =
        """
          |select * from table0 where not exists (select * from table1 where c = "odone")
          |""".stripMargin
      val ret6 = extractColumns(sql6)
      assert(ret6 == List(
          s"$DEFAULT_CATALOG.default.table0.a",
          s"$DEFAULT_CATALOG.default.table0.b",
          s"$DEFAULT_CATALOG.default.table0.c"))

      val sql7 =
        """
          |select * from table0 where table0.a not in (select a from table1)
          |""".stripMargin
      val ret7 = extractColumns(sql7)
      assert(ret7 ==
        List(
          s"$DEFAULT_CATALOG.default.table0.a",
          s"$DEFAULT_CATALOG.default.table0.b",
          s"$DEFAULT_CATALOG.default.table0.c"))

      val sql8 =
        """
          |select (select a from table1) + 1, b as bb from table1
          |""".stripMargin
      val ret8 = extractColumns(sql8)
      assert(ret8 == List(
          s"$DEFAULT_CATALOG.default.table1.a",
          s"$DEFAULT_CATALOG.default.table1.b"))

      val sql9 =
        """
          |select (select a from table1 limit 1) + 1 as aa, b as bb from table1
          |""".stripMargin
      val ret9 = extractColumns(sql9)
      assert(ret9 ==
        List(
          s"$DEFAULT_CATALOG.default.table1.a",
          s"$DEFAULT_CATALOG.default.table1.b"))

      val sql10 =
        """
          |select (select a from table1 limit 1) + (select a from table0 limit 1) + 1 as aa,
          | b as bb from table1
          |""".stripMargin
      val ret10 = extractColumns(sql10)
      assert(ret10 ==
        List(
          s"$DEFAULT_CATALOG.default.table1.a",
          s"$DEFAULT_CATALOG.default.table0.a",
          s"$DEFAULT_CATALOG.default.table1.b"))

      val sql11 =
        """
          |select tmp.a, b from (select * from table1) tmp;
          |""".stripMargin

      val ret11 = extractColumns(sql11)
      assert(ret11 ==
        List(
          s"$DEFAULT_CATALOG.default.table1.a",
          s"$DEFAULT_CATALOG.default.table1.b",
          s"$DEFAULT_CATALOG.default.table1.c"))
    }
  }

  test("test group by") {
    withTable("t1", "t2", "v2_catalog.db.t1", "v2_catalog.db.t2") { _ =>
      spark.sql("CREATE TABLE t1 (a string, b string, c string) USING hive")
      spark.sql("CREATE TABLE t2 (a string, b string, c string) USING hive")
      spark.sql("CREATE TABLE v2_catalog.db.t1 (a string, b string, c string)")
      spark.sql("CREATE TABLE v2_catalog.db.t2 (a string, b string, c string)")
      val ret0 =
        extractColumns(
          s"insert into table t1 select a," +
            s"concat_ws('/', collect_set(b))," +
            s"count(distinct(b)) * count(distinct(c))" +
            s"from t2 group by a")
      assert(ret0 ==
        List(
          s"$DEFAULT_CATALOG.default.t2.a",
          s"$DEFAULT_CATALOG.default.t2.b",
          s"$DEFAULT_CATALOG.default.t2.c"))

      val ret1 =
        extractColumns(
          s"insert into table v2_catalog.db.t1 select a," +
            s"concat_ws('/', collect_set(b))," +
            s"count(distinct(b)) * count(distinct(c))" +
            s"from v2_catalog.db.t2 group by a")
      assert(ret1 ==
        List(
          "v2_catalog.db.t2.a",
          "v2_catalog.db.t2.b",
          "v2_catalog.db.t2.c"))

      val ret2 =
        extractColumns(
          s"insert into table v2_catalog.db.t1 select a," +
            s"count(distinct(cast(b as int)+cast(c as int)))," +
            s"count(distinct(b)) * count(distinct(c))" +
            s"from v2_catalog.db.t2 group by a")
      assert(ret2 ==
        List(
          "v2_catalog.db.t2.a",
          "v2_catalog.db.t2.b",
          "v2_catalog.db.t2.c"))
    }
  }

  test("test grouping sets") {
    withTable("t1", "t2") { _ =>
      spark.sql("CREATE TABLE t1 (a string, b string, c string) USING hive")
      spark.sql("CREATE TABLE t2 (a string, b string, c string, d string) USING hive")
      val ret0 =
        extractColumns(
          s"insert into table t1 select a,b,GROUPING__ID " +
            s"from t2 group by a,b,c,d grouping sets ((a,b,c), (a,b,d))")
      assert(ret0 ==
        List(
          s"$DEFAULT_CATALOG.default.t2.a",
          s"$DEFAULT_CATALOG.default.t2.b",
          s"$DEFAULT_CATALOG.default.t2.c",
          s"$DEFAULT_CATALOG.default.t2.d",
          ))
    }
  }

  test("test cache table with window function") {
    withTable("t1", "t2") { _ =>
      spark.sql("CREATE TABLE t1 (a string, b string) USING hive")
      spark.sql("CREATE TABLE t2 (a string, b string) USING hive")

      spark.sql(
        s"cache table c1 select * from (" +
          s"select a, b, row_number() over (partition by a order by b asc ) rank from t2)" +
          s" where rank=1")
      val ret0 = extractColumns("insert overwrite table t1 select a, b from c1")
      assert(ret0 ==
        List(
          s"$DEFAULT_CATALOG.default.t2.a",
          s"$DEFAULT_CATALOG.default.t2.b"))

      val ret1 = extractColumns("insert overwrite table t1 select a, rank from c1")
      assert(ret1 ==
        List(
          s"$DEFAULT_CATALOG.default.t2.a",
          s"$DEFAULT_CATALOG.default.t2.b"))

      spark.sql(
        s"cache table c2 select * from (" +
          s"select b, a, row_number() over (partition by a order by b asc ) rank from t2)" +
          s" where rank=1")
      val ret2 = extractColumns("insert overwrite table t1 select a, b from c2")
      assert(ret2 ==
        List(
          s"$DEFAULT_CATALOG.default.t2.b",
          s"$DEFAULT_CATALOG.default.t2.a",
          ))

      spark.sql(
        s"cache table c3 select * from (" +
          s"select a as aa, b as bb, row_number() over (partition by a order by b asc ) rank" +
          s" from t2) where rank=1")
      val ret3 = extractColumns("insert overwrite table t1 select aa, bb from c3")
      assert(ret3 ==
        List(
          s"$DEFAULT_CATALOG.default.t2.a",
          s"$DEFAULT_CATALOG.default.t2.b"))
    }
  }

  test("test count()") {
    withTable("t1", "t2") { _ =>
      spark.sql("CREATE TABLE t1 (a string, b string, c string) USING hive")
      spark.sql("CREATE TABLE t2 (a string, b string, c string) USING hive")
      val ret0 = extractColumns("insert into t1 select 1,2,(select count(distinct" +
        " ifnull(get_json_object(a, '$.b.imei'), get_json_object(a, '$.b.android_id'))) from t2)")

      assert(ret0 ==
        List(
          s"$DEFAULT_CATALOG.default.t2.a"))
    }
  }

  test("test create view from view") {
    withTable("t1") { _ =>
      spark.sql("CREATE TABLE t1 (a string, b string, c string) USING hive")
      withView("t2") { _ =>
        spark.sql("CREATE VIEW t2 as select * from t1")
        val ret0 =
          extractColumns(
            s"create or replace view view_tst comment 'view'" +
              s" as select a as k,b" +
              s" from t2" +
              s" where a in ('HELLO') and c = 'HELLO'")
        assert(ret0 ==
          List(
            s"$DEFAULT_CATALOG.default.t1.a",
            s"$DEFAULT_CATALOG.default.t1.b",
            s"$DEFAULT_CATALOG.default.t1.c"))
      }
    }
  }

  test("test for skip parsing permanent view") {
    withTable("t1") { _ =>
      SparkContextHelper.setConf(LineageConf.SKIP_PARSING_PERMANENT_VIEW_ENABLED, true)
      spark.sql("CREATE TABLE t1 (a string, b string, c string) USING hive")
      withView("t2") { _ =>
        spark.sql("CREATE VIEW t2 as select * from t1")
        val ret0 =
          extractColumns(
            s"select a as k, b" +
              s" from t2" +
              s" where a in ('HELLO') and c = 'HELLO'")
        assert(ret0 ==
          List(
            s"$DEFAULT_CATALOG.default.t2.a",
            s"$DEFAULT_CATALOG.default.t2.b",
            s"$DEFAULT_CATALOG.default.t2.c",
          ))
      }
    }
  }

  test("test the statement with FROM xxx INSERT xxx") {
    withTable("t1", "t2", "t3") { _ =>
      spark.sql("CREATE TABLE t1 (a string, b string) USING hive")
      spark.sql("CREATE TABLE t2 (a string, b string) USING hive")
      spark.sql("CREATE TABLE t3 (a string, b string) USING hive")
      val ret0 = extractColumns("from (select a,b from t1)" +
        " insert overwrite table t2 select a,b where a=1" +
        " insert overwrite table t3 select a,b where b=1")
      assert(ret0 ==
        List(
          s"$DEFAULT_CATALOG.default.t1.a",
          s"$DEFAULT_CATALOG.default.t1.b",
          ))
    }
  }

  test("test lateral view explode") {
    withTable("t1", "t2") { _ =>
      spark.sql("CREATE TABLE t1 (a string, b string, c string, d string) USING hive")
      spark.sql("CREATE TABLE t2 (a string, b string, c string, d string) USING hive")

      val ret0 = extractColumns("insert into t1 select 1, t2.b, cc.action, t2.d " +
        "from t2 lateral view explode(split(c,'\\},\\{')) cc as action")
      assert(ret0 ==
        List(
          s"$DEFAULT_CATALOG.default.t2.b",
          s"$DEFAULT_CATALOG.default.t2.d",
          s"$DEFAULT_CATALOG.default.t2.c",
        ))

      val ret1 = extractColumns("insert into t1 select 1, t2.b, cc.action0, dd.action1 " +
        "from t2 " +
        "lateral view explode(split(c,'\\},\\{')) cc as action0 " +
        "lateral view explode(split(d,'\\},\\{')) dd as action1")
      assert(ret1 ==
        List(
          s"$DEFAULT_CATALOG.default.t2.b",
          s"$DEFAULT_CATALOG.default.t2.d",
          s"$DEFAULT_CATALOG.default.t2.c",
          ))

      val ret2 = extractColumns("insert into t1 select 1, t2.b, dd.pos, dd.action1 " +
        "from t2 " +
        "lateral view posexplode(split(d,'\\},\\{')) dd as pos, action1")
      assert(ret2 ==
        List(
          s"$DEFAULT_CATALOG.default.t2.b",
          s"$DEFAULT_CATALOG.default.t2.d"))
    }
  }

  test("test directory to table") {
    val inputFile = getClass.getResource("/").getPath + "input_file"
    val sourceFile = File(inputFile).createFile()
    withView("temp_view") { _ =>
    {
      spark.sql(
        s"""
           |CREATE OR REPLACE TEMPORARY VIEW temp_view (
           | `a` STRING COMMENT '',
           | `b` STRING COMMENT ''
           |) USING csv OPTIONS(
           |  sep='\t',
           |  path='${sourceFile.path}'
           |);
           |""".stripMargin).collect()

      val ret0 = extractColumnsWithoutExecuting(
        s"""
           |INSERT OVERWRITE TABLE test_db.test_table_from_dir
           |SELECT `a`, `b` FROM temp_view
           |""".stripMargin)

      assert(ret0 ==
        List())
    }
    }
  }

  test("columns lineage extract - collect input tables by plan") {
    val ddls =
      """
        |create table v2_catalog.db.tb1(col1 string, col2 string, col3 string)
        |create table v2_catalog.db.tb2(col1 string, col2 string, col3 string)
        |create table v2_catalog.db.tb3(col1 string, col2 string, col3 string)
        |""".stripMargin
    ddls.split("\n").filter(_.nonEmpty).foreach(spark.sql(_).collect())
    withTable("v2_catalog.db.tb1", "v2_catalog.db.tb2", "v2_catalog.db.tb3") { _ =>
      val sql0 =
        """
          |insert overwrite v2_catalog.db.tb3
          |select t1.col1, t1.col2 , t1.col3
          |from v2_catalog.db.tb1 t1 join v2_catalog.db.tb2 t2
          |on t1.col1 = t2.col1
          |""".stripMargin

      val ret0 = extractColumns(sql0)
      assert(
        ret0 ==
          List(
            "v2_catalog.db.tb1.col1",
            "v2_catalog.db.tb1.col2",
            "v2_catalog.db.tb1.col3",
            "v2_catalog.db.tb2.col1",
          ))

      val sql1 =
        """
          |insert overwrite v2_catalog.db.tb3
          |select t1.col1, t1.col2 , t1.col3
          |from v2_catalog.db.tb1 t1 left semi join v2_catalog.db.tb2 t2
          |on t1.col1 = t2.col1
          |""".stripMargin

      val ret1 = extractColumns(sql1)
      assert(
        ret1 == List(
          "v2_catalog.db.tb2.col1",
          "v2_catalog.db.tb1.col1",
          "v2_catalog.db.tb1.col2",
          "v2_catalog.db.tb1.col3",
        ))
    }
  }

  test("columns lineage extract - MergeIntoTable") {
    val ddls =
      """
        |create table v2_catalog.db.target_t(id int, name string, price float)
        |create table v2_catalog.db.source_t(id int, name string, price float)
        |create table v2_catalog.db.pivot_t(id int, price float)
        |""".stripMargin
    ddls.split("\n").filter(_.nonEmpty).foreach(spark.sql(_).collect())
    withTable("v2_catalog.db.target_t", "v2_catalog.db.source_t", "v2_catalog.db.pivot_t") { _ =>
      val ret0 = extractColumnsWithoutExecuting("MERGE INTO v2_catalog.db.target_t AS target " +
        "USING v2_catalog.db.source_t AS source " +
        "ON target.id = source.id " +
        "WHEN MATCHED THEN " +
        "  UPDATE SET target.name = source.name, target.price = source.price " +
        "WHEN NOT MATCHED THEN " +
        "  INSERT (id, name, price) VALUES (cast(source.id as int), source.name, source.price)")
      assert(ret0 ==
        List(
          "v2_catalog.db.target_t.name",
          "v2_catalog.db.target_t.price",
          "v2_catalog.db.target_t.id",
          "v2_catalog.db.source_t.name",
          "v2_catalog.db.source_t.price",
          "v2_catalog.db.source_t.id",
          ))

      val ret1 = extractColumnsWithoutExecuting("MERGE INTO v2_catalog.db.target_t AS target " +
        "USING v2_catalog.db.source_t AS source " +
        "ON target.id = source.id " +
        "WHEN MATCHED THEN " +
        "  UPDATE SET * " +
        "WHEN NOT MATCHED THEN " +
        "  INSERT *")
      assert(ret1 ==
        List(
          "v2_catalog.db.target_t.id",
          "v2_catalog.db.target_t.name",
          "v2_catalog.db.target_t.price",
          "v2_catalog.db.source_t.id",
          "v2_catalog.db.source_t.name",
          "v2_catalog.db.source_t.price"))

      val ret2 = extractColumnsWithoutExecuting("MERGE INTO v2_catalog.db.target_t AS target " +
        "USING (select a.id, a.name, b.price " +
        "from v2_catalog.db.source_t a join v2_catalog.db.pivot_t b) AS source " +
        "ON target.id = source.id " +
        "WHEN MATCHED THEN " +
        "  UPDATE SET * " +
        "WHEN NOT MATCHED THEN " +
        "  INSERT *")

      assert(ret2 ==
        List(
          "v2_catalog.db.target_t.id",
          "v2_catalog.db.target_t.name",
          "v2_catalog.db.target_t.price",
          "v2_catalog.db.source_t.id",
          "v2_catalog.db.source_t.name",
          "v2_catalog.db.pivot_t.price"))
    }
  }

  test("columns lineage extract - WriteDelta") {
    assume(
      SPARK_RUNTIME_VERSION >= "3.5",
      "WriteDelta is only supported in SPARK_RUNTIME_VERSION >= 3.5")
    val ddls =
      """
        |create table v2_catalog.db.target_t(pk int not null, name string, price float)
        | TBLPROPERTIES ('supports-deltas'='true');
        |create table v2_catalog.db.source_t(pk int not null, name string, price float)
        | TBLPROPERTIES ('supports-deltas'='true');
        |create table v2_catalog.db.pivot_t(pk int not null, price float)
        | TBLPROPERTIES ('supports-deltas'='true')
        |""".stripMargin
    ddls.split(";").filter(_.nonEmpty).foreach(spark.sql(_).collect())

    withTable("v2_catalog.db.target_t", "v2_catalog.db.source_t", "v2_catalog.db.pivot_t") { _ =>
      val ret0 = extractColumnsWithoutExecuting(
        "MERGE INTO v2_catalog.db.target_t AS target " +
          "USING v2_catalog.db.source_t AS source " +
          "ON target.pk = source.pk " +
          "WHEN MATCHED THEN " +
          "  UPDATE SET target.name = source.name, target.price = source.price " +
          "WHEN NOT MATCHED THEN " +
          "  INSERT (pk, name, price) VALUES (cast(source.pk as int), source.name, source.price)" +
          "WHEN NOT MATCHED BY SOURCE THEN  UPDATE SET target.name = 'abc' ")
      assert(ret0 ==
        List(
          "v2_catalog.db.target_t.name",
          "v2_catalog.db.target_t.price",
          "v2_catalog.db.target_t.pk",
          "v2_catalog.db.source_t.name",
          "v2_catalog.db.source_t.price",
          "v2_catalog.db.source_t.pk",
        ))

      val ret1 = extractColumnsWithoutExecuting(
        "MERGE INTO v2_catalog.db.target_t AS target " +
          "USING v2_catalog.db.source_t AS source " +
          "ON target.pk = source.pk " +
          "WHEN MATCHED THEN " +
          "  UPDATE SET * " +
          "WHEN NOT MATCHED THEN " +
          "  INSERT *")
      assert(ret1 ==
        List(
          "v2_catalog.db.target_t.pk",
          "v2_catalog.db.target_t.name",
          "v2_catalog.db.target_t.price",
          "v2_catalog.db.source_t.pk",
          "v2_catalog.db.source_t.name",
          "v2_catalog.db.source_t.price"))

      val ret2 = extractColumnsWithoutExecuting(
        "MERGE INTO v2_catalog.db.target_t AS target " +
          "USING (select a.pk, a.name, b.price " +
          "from v2_catalog.db.source_t a join " +
          "v2_catalog.db.pivot_t b) AS source " +
          "ON target.pk = source.pk " +
          "WHEN MATCHED THEN " +
          "  UPDATE SET * " +
          "WHEN NOT MATCHED THEN " +
          "  INSERT *")

      assert(ret2 ==
        List(
          "v2_catalog.db.target_t.pk",
          "v2_catalog.db.target_t.name",
          "v2_catalog.db.target_t.price",
          "v2_catalog.db.source_t.pk",
          "v2_catalog.db.source_t.name",
          "v2_catalog.db.pivot_t.price"))

      val ret3 = extractColumnsWithoutExecuting(
        "update v2_catalog.db.target_t AS set name='abc' where price < 10 ")
      assert(ret3 ==
        List(
          "v2_catalog.db.target_t.pk",
          "v2_catalog.db.target_t.name",
          "v2_catalog.db.target_t.price"))
    }
  }

  test("columns lineage extract - ReplaceData") {
    assume(
      SPARK_RUNTIME_VERSION >= "3.5",
      "ReplaceData[SPARK-43963] for merge into is supported in SPARK_RUNTIME_VERSION >= 3.5")
    val ddls =
      """
        |create table v2_catalog.db.target_t(id int, name string, price float)
        |create table v2_catalog.db.source_t(id int, name string, price float)
        |create table v2_catalog.db.pivot_t(id int, price float)
        |""".stripMargin
    ddls.split("\n").filter(_.nonEmpty).foreach(spark.sql(_).collect())
    withTable("v2_catalog.db.target_t", "v2_catalog.db.source_t", "v2_catalog.db.pivot_t") { _ =>
      val ret0 = extractColumnsWithoutExecuting("MERGE INTO v2_catalog.db.target_t AS target " +
        "USING v2_catalog.db.source_t AS source " +
        "ON target.id = source.id " +
        "WHEN MATCHED THEN " +
        "  UPDATE SET target.name = source.name, target.price = source.price " +
        "WHEN NOT MATCHED THEN " +
        "  INSERT (id, name, price) VALUES (cast(source.id as int), source.name, source.price)")

      /**
       * The ReplaceData operation requires that target records which are read but do not match
       * any of the MATCHED or NOT MATCHED BY SOURCE clauses also be copied.
       * (refer to [[RewriteMergeIntoTable#buildReplaceDataMergeRowsPlan]])
       */
      assert(ret0 ==
        List(
          "v2_catalog.db.target_t.name",
          "v2_catalog.db.target_t.price",
          "v2_catalog.db.target_t.id",
          "v2_catalog.db.source_t.name",
          "v2_catalog.db.source_t.price",
          "v2_catalog.db.source_t.id",
        ))

      val ret1 = extractColumnsWithoutExecuting("MERGE INTO v2_catalog.db.target_t AS target " +
        "USING v2_catalog.db.source_t AS source " +
        "ON target.id = source.id " +
        "WHEN MATCHED THEN " +
        "  UPDATE SET * " +
        "WHEN NOT MATCHED THEN " +
        "  INSERT *")
      assert(ret1 ==
        List(
          "v2_catalog.db.target_t.id",
          "v2_catalog.db.target_t.name",
          "v2_catalog.db.target_t.price",
          "v2_catalog.db.source_t.id",
          "v2_catalog.db.source_t.name",
          "v2_catalog.db.source_t.price",
        ))

      val ret2 = extractColumnsWithoutExecuting("MERGE INTO v2_catalog.db.target_t AS target " +
        "USING (select a.id, a.name, b.price " +
        "from v2_catalog.db.source_t a join v2_catalog.db.pivot_t b) AS source " +
        "ON target.id = source.id " +
        "WHEN MATCHED THEN " +
        "  UPDATE SET * " +
        "WHEN NOT MATCHED THEN " +
        "  INSERT *")

      assert(ret2 ==
        List(
          "v2_catalog.db.target_t.id",
          "v2_catalog.db.target_t.name",
          "v2_catalog.db.target_t.price",
          "v2_catalog.db.source_t.id",
          "v2_catalog.db.source_t.name",
          "v2_catalog.db.pivot_t.price",
        ))

      val ret3 = extractColumnsWithoutExecuting(
        "update v2_catalog.db.target_t AS set name='abc' where price < 10 ")
      // For tables that do not support row-level deletion,
      // duplicate data of the same group may be included when writing.
      // plan is:
      // ReplaceData
      // +- Project [if ((price#1160 < cast(10 as float))) id#1158 else id#1158 AS id#1163,
      //    if ((price#1160 < cast(10 as float))) abc else name#1159 AS name#1164,
      //    if ((price#1160 < cast(10 as float))) price#1160 else price#1160 AS price#1165,
      //    _partition#1162]
      // +- RelationV2[id#1158, name#1159, price#1160, _partition#1162]
      //    v2_catalog.db.target_t v2_catalog.db.target_t
      assert(ret3 ==
        List(
          "v2_catalog.db.target_t.id",
          "v2_catalog.db.target_t.name",
          "v2_catalog.db.target_t.price"))
    }
  }

  private def extractColumns(sql: String): List[String] = {
    val parsed = spark.sessionState.sqlParser.parsePlan(sql)
    val qe = spark.sessionState.executePlan(parsed)
    val analyzed = qe.analyzed
    SparkSQLColumnParseHelper(spark).extractColumn(0, analyzed).get
  }

  protected def extractColumnsWithoutExecuting(sql: String): List[String] = {
    val parsed = spark.sessionState.sqlParser.parsePlan(sql)
    val analyzed = spark.sessionState.analyzer.execute(parsed)
    spark.sessionState.analyzer.checkAnalysis(analyzed)
    SparkSQLColumnParseHelper(spark).extractColumn(0, analyzed).get
  }
}

