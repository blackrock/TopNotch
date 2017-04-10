package com.bfm.topnotch.tndiff

import java.sql.{Date, Timestamp}

import com.bfm.topnotch.SparkApplicationTester
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.scalatest.{Matchers, Tag}

/**
 * The tests for [[com.bfm.topnotch.tndiff.TnDiffCreator TnDiffCreator]].
 */
class TnDiffCreatorTest extends SparkApplicationTester with Matchers {
  import com.bfm.topnotch.TnTestHelper._
  import TnDiffCreator._

  lazy val diffCreator = new TnDiffCreator()

  /**
   * Create dataframes containing every possible type that can be stored in the dataframe
   */

  case class NumericTestClass(
                               id: Option[Int],
                               id2: Option[Int],
                               byte1: Option[Byte],
                               byte2: Option[Byte],
                               short1: Option[Short],
                               short2: Option[Short],
                               int1: Option[Int],
                               int2: Option[Int],
                               long1: Option[Long],
                               long2: Option[Long],
                               float1: Option[Float],
                               float2: Option[Float],
                               double1: Option[Double],
                               double2: Option[Double],
                               decimal1: Option[BigDecimal],
                               decimal2: Option[BigDecimal]
                               )

  case class Inner(
                    int: Int
                    )

  case class OtherTestClass(
                             id: Option[Int],
                             id2: Option[Int],
                             string1: Option[String],
                             string2: Option[String],
                             binary1: Option[Array[Byte]],
                             binary2: Option[Array[Byte]],
                             boolean1: Option[Boolean],
                             boolean2: Option[Boolean],
                             timestamp1: Option[Timestamp],
                             timestamp2: Option[Timestamp],
                             date1: Option[Date],
                             date2: Option[Date],
                             array1: Option[Seq[Int]],
                             array2: Option[Seq[Int]],
                             map1: Option[Map[Int, Int]],
                             map2: Option[Map[Int, Int]],
                             struct1: Option[Inner],
                             struct2: Option[Inner]
                             )

  case class MixTestClass(
                           id: Int,
                           decimal: Option[BigDecimal],
                           map: Option[Map[Int, Int]]
                           )

  case class NumericThresholdClass(
                                    id: Int,
                                    double1: Double,
                                    decimal1: BigDecimal
                                    ) {
    def perturb(epsilon: Double) = this.copy(double1 = this.double1 + epsilon, decimal1 = decimal1 + epsilon)
  }

  /**
   * The data points to compare that have different values.
   * Numeric points hold the numeric data types and other points hold all other data types.
   * All filled means that there are no nulls in the data point. Half filled means that half the values are null.
   */
  lazy val numericAllFilledTestPoint = NumericTestClass(Some(1), Some(11), Some(0x1: Byte), Some(0x2: Byte), Some(3: Short),
    Some(4: Short), Some(5), Some(6), Some(7L), Some(8L), Some(9.0F), Some(10.0F), Some(11.0D), Some(12.0D),
    Some(BigDecimal(13.0)), Some(BigDecimal(14.0)))

  lazy val otherAllFilledTestPoint = OtherTestClass(Some(2), Some(12), Some("15"), Some("16"), Some(Array(0xA: Byte)), Some(Array(0xB: Byte)),
    Some(true), Some(false), Some(new Timestamp(17)), Some(new Timestamp(18)), Some(new Date(19)), Some(new Date(20)),
    Some(Seq(21)), Some(Seq(22)), Some(Map((23, 24))), Some(Map((25, 26))), Some(Inner(27)), Some(Inner(28)))

  lazy val numericHalfFilledTestPoint = NumericTestClass(Some(3), Some(13), Some(0x1: Byte), None, Some(3: Short), None, Some(5), None, Some(7L),
    None, Some(9.0F), None, Some(11.0D), None, Some(BigDecimal(13.0)), None)

  lazy val otherHalfFilledTestPoint = OtherTestClass(Some(4), Some(14), Some("15"), None, Some(Array(0xA: Byte)), None, Some(true), None,
    Some(new Timestamp(17)), None, Some(new Date(19)), None, Some(Seq(21)), None, Some(Map((23, 24))), None, Some(Inner(27)), None)

  /**
   * The data frames holding the data points with different values. 
   * null means that the data frame contains the data points with nulls. noNull means that the data frame contains
   * the data points with no nulls. twoPoint means that the dataframe contains both
   */
  lazy val twoPointNumericDf = spark.createDataFrame(Seq(numericAllFilledTestPoint, numericHalfFilledTestPoint)).cache

  lazy val noNullNumericDf = twoPointNumericDf.filter(col("byte2").isNotNull).cache

  lazy val nullNumericDf = twoPointNumericDf.filter(col("byte2").isNull).cache

  lazy val twoPointOtherDf = spark.createDataFrame(Seq(otherAllFilledTestPoint, otherHalfFilledTestPoint)).cache

  lazy val noNullOtherDf = twoPointOtherDf.filter(col("string2").isNotNull).cache

  /**
   * The data points to compare that have equal values
   */
  lazy val numericEqualityTestPoint = NumericTestClass(Some(1), Some(1), Some(0x1: Byte), Some(0x1: Byte), Some(1: Short),
    Some(1: Short), Some(1), Some(1), Some(1L), Some(1L), Some(1.0F), Some(1.0F), Some(1.0D), Some(1.0D),
    Some(BigDecimal(1.0)), Some(BigDecimal(1.0)))

  lazy val otherEqualityTestPoint = OtherTestClass(Some(1), Some(1), Some("1"), Some("1"), Some(Array(0x1: Byte)), Some(Array(0x1: Byte)),
    Some(true), Some(true), Some(new Timestamp(1)), Some(new Timestamp(1)), Some(new Date(1)), Some(new Date(1)),
    Some(Seq(1)), Some(Seq(1)), Some(Map((1, 1))), Some(Map((1, 1))), Some(Inner(1)), Some(Inner(1)))
  
  lazy val numericThresholdPoint = NumericThresholdClass(1, 0.05, BigDecimal(0.001))

  /**
   * The dataframes holding the equal points
   */
  lazy val numericEqualityDf = spark.createDataFrame(Seq(numericEqualityTestPoint))

  lazy val otherEqualityDf = spark.createDataFrame(Seq(otherEqualityTestPoint))

  lazy val numericThresholdTestDf = spark.createDataFrame(Seq(numericThresholdPoint))

  lazy val numericThresholdTestDfPerturbed = spark.createDataFrame(Seq(numericThresholdPoint.perturb(1e-6)))

  /**
   * The dataframe for testing points of different types
    */
  lazy val mixDf = spark.createDataFrame(Seq(MixTestClass(1, Some(BigDecimal(1)), Some(Map((23, 2))))))

  /**
   * These dataframes are useful as they allow for testing non-unique keys
   */
  lazy val currentLoansDF = spark.read.parquet(getClass.getResource("currentLoans.parquet").getFile).cache

  lazy val oldLoansDF = spark.read.parquet(getClass.getResource("oldLoans.parquet").getFile).cache
  

  /**
   * The tags
   */
  object generateDiffTag extends Tag("createDiff")

  /**
   * The tests for GenerateDiff
   */
  "GenerateDiff" should "throw an exception when given no join columns" taggedAs generateDiffTag in {
    intercept[IllegalArgumentException] {
      diffCreator.createDiff(twoPointNumericDf, "t1", twoPointNumericDf, "t2",
        TnDiffParams(TnDiffInput(Seq(), Seq()), TnDiffInput(Seq(), Seq())))
    }
  }

  it should "throw an exception when given different numbers of join columns" taggedAs generateDiffTag in {
    intercept[IllegalArgumentException] {
      diffCreator.createDiff(twoPointNumericDf, "t1", twoPointNumericDf, "t2",
        TnDiffParams(TnDiffInput(Seq("a", "b"), Seq("a")), TnDiffInput(Seq("a"), Seq("a"))))
    }
  }

  it should "throw an exception when given different numbers of diff columns" taggedAs generateDiffTag in {
    intercept[IllegalArgumentException] {
      diffCreator.createDiff(twoPointNumericDf, "t1", twoPointNumericDf, "t2",
        TnDiffParams(TnDiffInput(Seq("a"), Seq("a", "b")), TnDiffInput(Seq("a"), Seq("a"))))
    }
  }

  it should "throw an exception when given join columns which don't form a unique key" taggedAs generateDiffTag in {
    intercept[IllegalArgumentException] {
      diffCreator.createDiff(currentLoansDF, "t1", oldLoansDF, "t2",
        TnDiffParams(TnDiffInput(Seq("loanID"), Seq("loanBal", "loanFICO")),
          TnDiffInput(Seq("loanIDOld", "poolNumOld"), Seq("loanBalOld", "loanFICOOld"))))
    }
    intercept[IllegalArgumentException] {
      diffCreator.createDiff(currentLoansDF, "t1", oldLoansDF, "t2",
        TnDiffParams(TnDiffInput(Seq("loanID", "poolNum"), Seq("loanBal", "loanFICO")),
          TnDiffInput(Seq("poolNumOld"), Seq("loanBalOld", "loanFICOOld"))))
    }
  }

  it should "throw an exception when given join columns contains a null" taggedAs generateDiffTag in {
    intercept[IllegalArgumentException] {
      diffCreator.createDiff(twoPointNumericDf, "t1", noNullNumericDf, "t2",
        TnDiffParams(TnDiffInput(Seq("int2"), Seq("int1")), TnDiffInput(Seq("int2"), Seq("int1"))))
    }
    intercept[IllegalArgumentException] {
      diffCreator.createDiff(noNullNumericDf, "t1", twoPointNumericDf, "t2",
        TnDiffParams(TnDiffInput(Seq("int2"), Seq("int1")), TnDiffInput(Seq("int2"), Seq("int1"))))
    }
  }

  it should "return an empty dataframe with just the join columns when given no diff columns" taggedAs generateDiffTag in {
    val diffDf = diffCreator.createDiff(twoPointNumericDf, "t1", twoPointOtherDf, "t2",
      TnDiffParams(TnDiffInput(Seq("id"), Seq()), TnDiffInput(Seq("id"), Seq())))
    dfEquals(diffDf, twoPointNumericDf.where(lit(false)).select(col("id").as(s"t1${colJoin}id"), col("id").as(s"t2${colJoin}id")))
  }

  it should "return an empty dataframe when diffing two identical dataframes and filterEqualRows true" taggedAs generateDiffTag in {
    val diffDf = diffCreator.createDiff(twoPointOtherDf, "t1", twoPointOtherDf, "t2",
      TnDiffParams(TnDiffInput(Seq("id"), Seq("string1")), TnDiffInput(Seq("id"), Seq("string1"))), filterEqualRows = true)
    dfEquals(diffDf, twoPointOtherDf.where(lit(false)).select(
      col("id").as(s"t1${colJoin}id"), col("id").as(s"t2${colJoin}id"),
      col("string1").as(s"t1${colJoin}string1"), col("string1").as(s"t2${colJoin}string1"),
      lit(equalStr).as(equalityColName(s"t1${colJoin}string1", s"t2${colJoin}string1"))))
  }

  it should "return not return equal rows when filterEqualRows is true" taggedAs generateDiffTag in {
    val diffDf = diffCreator.createDiff(twoPointOtherDf, "t1", twoPointOtherDf, "t2",
      TnDiffParams(TnDiffInput(Seq("id"), Seq("string1")), TnDiffInput(Seq("id"), Seq("string1"))), filterEqualRows = true)
    diffDf.count() shouldBe 0
  }

  it should "return an outer join of rows when filterEqualRows is false" taggedAs generateDiffTag in {
    val diffDf = diffCreator.createDiff(twoPointOtherDf, "t1", noNullOtherDf, "t2",
      TnDiffParams(TnDiffInput(Seq("id"), Seq("id2")), TnDiffInput(Seq("id"), Seq("id2"))))
    diffDf.count() shouldBe 2
  }

  it should "return an outer join of rows without equal ones when filterEqualRows is true" taggedAs generateDiffTag in {
    val diffDf = diffCreator.createDiff(twoPointNumericDf, "t1", twoPointOtherDf, "t2",
      TnDiffParams(TnDiffInput(Seq("id"), Seq("id2")), TnDiffInput(Seq("id"), Seq("id2"))), 0, filterEqualRows = true)
    diffDf.count() shouldBe 4
  }

  it should "return equals for one join column and one numeric diff column where the values are equal and of same type" taggedAs generateDiffTag in {
    val diffDf = diffCreator.createDiff(twoPointNumericDf, "t1", twoPointNumericDf, "t2",
      TnDiffParams(TnDiffInput(Seq("id"), Seq("int1")), TnDiffInput(Seq("id"), Seq("int1"))))
    dfEquals(diffDf, twoPointNumericDf.select(
      col("id").as(s"t1${colJoin}id"), col("id").as(s"t2${colJoin}id"),
      col("int1").as(s"t1${colJoin}int1"), col("int1").as(s"t2${colJoin}int1"),
      lit(0).as(minusColName(s"t1${colJoin}int1", s"t2${colJoin}int1")), lit(equalStr).as(equalityColName(s"t1${colJoin}int1", s"t2${colJoin}int1"))
    ))
  }

  it should "return equals for two join columns and a numeric diff column where the values are equal and of same type" taggedAs generateDiffTag in {
    val diffDf = diffCreator.createDiff(twoPointNumericDf, "t1", twoPointNumericDf, "t2",
      TnDiffParams(TnDiffInput(Seq("id", "id2"), Seq("int1")), TnDiffInput(Seq("id", "id2"), Seq("int1"))))
    dfEquals(diffDf, twoPointNumericDf.select(
      col("id").as(s"t1${colJoin}id"), col("id").as(s"t2${colJoin}id"),
      col("id2").as(s"t1${colJoin}id2"), col("id2").as(s"t2${colJoin}id2"),
      col("int1").as(s"t1${colJoin}int1"), col("int1").as(s"t2${colJoin}int1"),
      lit(0).as(minusColName(s"t1${colJoin}int1", s"t2${colJoin}int1")), lit(equalStr).as(equalityColName(s"t1${colJoin}int1", s"t2${colJoin}int1"))
    ))
  }

  it should "return appropriate diff values for two join columns and three numeric diff columns where values and types may " +
    "be equal or different but not null" taggedAs generateDiffTag in {
    val joinCols = Seq("id", "id2")
    val diffDf = diffCreator.createDiff(noNullNumericDf, "t1", noNullNumericDf, "t2", TnDiffParams(
      //the first diff columns should be equal, the second should be different but of the same type, third should be
      //different and of same type, all numeric
      TnDiffInput(joinCols, Seq("byte1", "short1", "int1")),
      TnDiffInput(joinCols, Seq("byte1", "short2", "decimal1"))))
    dfEquals(diffDf, noNullNumericDf.select(
      col("id").as(s"t1${colJoin}id"), col("id").as(s"t2${colJoin}id"),
      col("id2").as(s"t1${colJoin}id2"), col("id2").as(s"t2${colJoin}id2"),
      col("byte1").as(s"t1${colJoin}byte1"), col("byte1").as(s"t2${colJoin}byte1"),
      lit(0x0: Byte).as(minusColName(s"t1${colJoin}byte1", s"t2${colJoin}byte1")), lit(equalStr).as(equalityColName(s"t1${colJoin}byte1", s"t2${colJoin}byte1")),
      col("short1").as(s"t1${colJoin}short1"), col("short2").as(s"t2${colJoin}short2"),
      lit(-1: Short).as(minusColName(s"t1${colJoin}short1", s"t2${colJoin}short2")), lit(diffStr).as(equalityColName(s"t1${colJoin}short1", s"t2${colJoin}short2")),
      col("int1").as(s"t1${colJoin}int1"), col("decimal1").as(s"t2${colJoin}decimal1"),
      lit(BigDecimal(-8)).cast(new DecimalType(38, 18)).as(minusColName(s"t1${colJoin}int1", s"t2${colJoin}decimal1")), lit(diffTypeStr).as(equalityColName(s"t1${colJoin}int1", s"t2${colJoin}decimal1"))
    ))
  }

  it should "return appropriate diff values for two join columns and three numeric diff columns where values and types may " +
    "be equal or null" taggedAs generateDiffTag in {
    val joinCols = Seq("id", "id2")
    val diffDf = diffCreator.createDiff(nullNumericDf, "t1", nullNumericDf, "t2", TnDiffParams(
      TnDiffInput(joinCols, Seq("byte1", "short1", "long2", "double2", "int1", "float2")),
      TnDiffInput(joinCols, Seq("byte1", "short2", "long1", "double2", "decimal2", "long2"))))
    dfEquals(diffDf, nullNumericDf.select(
      col("id").as(s"t1${colJoin}id"), col("id").as(s"t2${colJoin}id"),
      col("id2").as(s"t1${colJoin}id2"), col("id2").as(s"t2${colJoin}id2"),
      col("byte1").as(s"t1${colJoin}byte1"), col("byte1").as(s"t2${colJoin}byte1"),
      lit(0x0: Byte).as(minusColName(s"t1${colJoin}byte1", s"t2${colJoin}byte1")), lit(equalStr).as(equalityColName(s"t1${colJoin}byte1", s"t2${colJoin}byte1")),
      col("short1").as(s"t1${colJoin}short1"), col("short2").as(s"t2${colJoin}short2"),
      lit(null).cast(ShortType).as(minusColName(s"t1${colJoin}short1", s"t2${colJoin}short2")), lit(secondNullStr).as(equalityColName(s"t1${colJoin}short1", s"t2${colJoin}short2")),
      col("long2").as(s"t1${colJoin}long2"), col("long1").as(s"t2${colJoin}long1"),
      lit(null).cast(LongType).as(minusColName(s"t1${colJoin}long2", s"t2${colJoin}long1")), lit(firstNullStr).as(equalityColName(s"t1${colJoin}long2", s"t2${colJoin}long1")),
      col("double2").as(s"t1${colJoin}double2"), col("double2").as(s"t2${colJoin}double2"),
      lit(null).cast(DoubleType).as(minusColName(s"t1${colJoin}double2", s"t2${colJoin}double2")), lit(bothNullStr).as(equalityColName(s"t1${colJoin}double2", s"t2${colJoin}double2")),
      col("int1").as(s"t1${colJoin}int1"), col("decimal2").as(s"t2${colJoin}decimal2"),
      lit(null).cast(new DecimalType(38, 18)).as(minusColName(s"t1${colJoin}int1", s"t2${colJoin}decimal2")), lit(secondNullStr).as(equalityColName(s"t1${colJoin}int1", s"t2${colJoin}decimal2")),
      col("float2").as(s"t1${colJoin}float2"), col("long2").as(s"t2${colJoin}long2"),
      lit(null).cast(FloatType).as(minusColName(s"t1${colJoin}float2", s"t2${colJoin}long2")), lit(bothNullStr).as(equalityColName(s"t1${colJoin}float2", s"t2${colJoin}long2"))
    ))
  }

  it should "return appropriate diff values for two join columns and one non-numeric diff column where the values are equal" taggedAs generateDiffTag in {
    val joinCols = Seq("id", "id2")
    val diffDf = diffCreator.createDiff(twoPointOtherDf, "t1", twoPointOtherDf, "t2", TnDiffParams(
      TnDiffInput(joinCols, Seq("string1")),
      TnDiffInput(joinCols, Seq("string1"))))
    dfEquals(diffDf, twoPointOtherDf.select(
      col("id").as(s"t1${colJoin}id"), col("id").as(s"t2${colJoin}id"),
      col("id2").as(s"t1${colJoin}id2"), col("id2").as(s"t2${colJoin}id2"),
      col("string1").as(s"t1${colJoin}string1"), col("string1").as(s"t2${colJoin}string1"),
      lit(equalStr).as(equalityColName(s"t1${colJoin}string1", s"t2${colJoin}string1"))
    ))
  }

  it should "return appropriate diff values for two join columns and one non-numeric diff column where the values are different" taggedAs generateDiffTag in {
    val joinCols = Seq("id", "id2")
    val diffDf = diffCreator.createDiff(noNullOtherDf, "t1", noNullOtherDf, "t2", TnDiffParams(
      TnDiffInput(joinCols, Seq("array1")),
      TnDiffInput(joinCols, Seq("array2"))))
    dfEquals(diffDf, noNullOtherDf.select(
      col("id").as(s"t1${colJoin}id"), col("id").as(s"t2${colJoin}id"),
      col("id2").as(s"t1${colJoin}id2"), col("id2").as(s"t2${colJoin}id2"),
      col("array1").as(s"t1${colJoin}array1"), col("array2").as(s"t2${colJoin}array2"),
      lit(diffStr).as(equalityColName(s"t1${colJoin}array1", s"t2${colJoin}array2"))
    ))
  }

  it should "return appropriate diff values for two join columns and one non-numeric diff column of different types" taggedAs generateDiffTag in {
    val joinCols = Seq("id", "id2")
    val diffDf = diffCreator.createDiff(noNullOtherDf, "t1", noNullOtherDf, "t2", TnDiffParams(
      TnDiffInput(joinCols, Seq("array1")),
      TnDiffInput(joinCols, Seq("string2"))))
    dfEquals(diffDf, noNullOtherDf.select(
      col("id").as(s"t1${colJoin}id"), col("id").as(s"t2${colJoin}id"),
      col("id2").as(s"t1${colJoin}id2"), col("id2").as(s"t2${colJoin}id2"),
      col("array1").as(s"t1${colJoin}array1"), col("string2").as(s"t2${colJoin}string2"),
      lit(diffTypeStr).as(equalityColName(s"t1${colJoin}array1", s"t2${colJoin}string2"))
    ))
  }

  it should "return different type when diffing a numeric column with a non-numeric one" taggedAs generateDiffTag in {
    val joinCols = Seq("id")
    val diffDf = diffCreator.createDiff(mixDf, "t1", mixDf, "t2", TnDiffParams(
      TnDiffInput(joinCols, Seq("decimal")),
      TnDiffInput(joinCols, Seq("map"))))
    dfEquals(diffDf, mixDf.select(
      col("id").as(s"t1${colJoin}id"), col("id").as(s"t2${colJoin}id"),
      col("decimal").as(s"t1${colJoin}decimal"), col("map").as(s"t2${colJoin}map"),
      lit(diffTypeStr).as(equalityColName(s"t1${colJoin}decimal", s"t2${colJoin}map"))
    ))
  }

  it should "return equals for all equal numeric values" taggedAs generateDiffTag in {
    val joinCols = Seq("id", "id2")
    val diffDf = diffCreator.createDiff(numericEqualityDf, "t1", numericEqualityDf, "t2", TnDiffParams(
      TnDiffInput(joinCols, Seq("byte1", "short1", "int1", "long1", "float1", "double1", "decimal1")),
      TnDiffInput(joinCols, Seq("byte2", "short2", "int2", "long2", "float2", "double2", "decimal2"))))
    dfEquals(diffDf, numericEqualityDf.select(
      col("id").as(s"t1${colJoin}id"), col("id").as(s"t2${colJoin}id"),
      col("id2").as(s"t1${colJoin}id2"), col("id2").as(s"t2${colJoin}id2"),
      col("byte1").as(s"t1${colJoin}byte1"), col("byte2").as(s"t2${colJoin}byte2"),
      lit(0: Byte).as(minusColName(s"t1${colJoin}byte1", s"t2${colJoin}byte2")), lit(equalStr).as(equalityColName(s"t1${colJoin}byte1", s"t2${colJoin}byte2")),
      col("short1").as(s"t1${colJoin}short1"), col("short2").as(s"t2${colJoin}short2"),
      lit(0: Short).as(minusColName(s"t1${colJoin}short1", s"t2${colJoin}short2")), lit(equalStr).as(equalityColName(s"t1${colJoin}short1", s"t2${colJoin}short2")),
      col("int1").as(s"t1${colJoin}int1"), col("int2").as(s"t2${colJoin}int2"),
      lit(0).as(minusColName(s"t1${colJoin}int1", s"t2${colJoin}int2")), lit(equalStr).as(equalityColName(s"t1${colJoin}int1", s"t2${colJoin}int2")),
      col("long1").as(s"t1${colJoin}long1"), col("long2").as(s"t2${colJoin}long2"),
      lit(0L).as(minusColName(s"t1${colJoin}long1", s"t2${colJoin}long2")), lit(equalStr).as(equalityColName(s"t1${colJoin}long1", s"t2${colJoin}long2")),
      col("float1").as(s"t1${colJoin}float1"), col("float2").as(s"t2${colJoin}float2"),
      lit(0.0F).as(minusColName(s"t1${colJoin}float1", s"t2${colJoin}float2")), lit(equalStr).as(equalityColName(s"t1${colJoin}float1", s"t2${colJoin}float2")),
      col("double1").as(s"t1${colJoin}double1"), col("double2").as(s"t2${colJoin}double2"),
      lit(0.0).as(minusColName(s"t1${colJoin}double1", s"t2${colJoin}double2")), lit(equalStr).as(equalityColName(s"t1${colJoin}double1", s"t2${colJoin}double2")),
      col("decimal1").as(s"t1${colJoin}decimal1"), col("decimal2").as(s"t2${colJoin}decimal2"),
      lit(BigDecimal(0)).cast(new DecimalType(38, 18)).as(minusColName(s"t1${colJoin}decimal1", s"t2${colJoin}decimal2")), lit(equalStr).as(equalityColName(s"t1${colJoin}decimal1", s"t2${colJoin}decimal2"))

    ))
  }

  it should "return equals for all equal non-numeric, non-complex values" taggedAs generateDiffTag in {
    val joinCols = Seq("id", "id2")
    val diffDf = diffCreator.createDiff(otherEqualityDf, "t1", otherEqualityDf, "t2", TnDiffParams(
      TnDiffInput(joinCols, Seq("string1", "binary1", "boolean1", "timestamp1", "date1")),
      TnDiffInput(joinCols, Seq("string2", "binary2", "boolean2", "timestamp2", "date2"))))
    //removing these as my dfEquals function can't handle that level of nested arrays, As long as the TnDiffCreator returns
    //the correct string in the equals column, the test passes
    dfEquals(diffDf.drop(s"t1${colJoin}binary1").drop(s"t2${colJoin}binary2"), otherEqualityDf.select(
      col("id").as(s"t1${colJoin}id"), col("id").as(s"t2${colJoin}id"),
      col("id2").as(s"t1${colJoin}id2"), col("id2").as(s"t2${colJoin}id2"),
      col("string1").as(s"t1${colJoin}string1"), col("string2").as(s"t2${colJoin}string2"),
      lit(equalStr).as(equalityColName(s"t1${colJoin}string1", s"t2${colJoin}string2")),
      lit(equalStr).as(equalityColName(s"t1${colJoin}binary1", s"t2${colJoin}binary2")),
      col("boolean1").as(s"t1${colJoin}boolean1"), col("boolean2").as(s"t2${colJoin}boolean2"),
      lit(equalStr).as(equalityColName(s"t1${colJoin}boolean1", s"t2${colJoin}boolean2")),
      col("timestamp1").as(s"t1${colJoin}timestamp1"), col("timestamp2").as(s"t2${colJoin}timestamp2"),
      lit(equalStr).as(equalityColName(s"t1${colJoin}timestamp1", s"t2${colJoin}timestamp2")),
      col("date1").as(s"t1${colJoin}date1"), col("date2").as(s"t2${colJoin}date2"),
      lit(equalStr).as(equalityColName(s"t1${colJoin}date1", s"t2${colJoin}date2"))

    ))
  }
  it should "return equals for both the perturbed data set within the threshold" taggedAs generateDiffTag in {
    val joinCols = Seq("id")
    val diffedDf = diffCreator.createDiff(numericThresholdTestDf, "t1", numericThresholdTestDfPerturbed, "t2", TnDiffParams(
      TnDiffInput(joinCols, Seq("double1", "decimal1")),
      TnDiffInput(joinCols, Seq("double1", "decimal1"))), 1e-5, true)

    dfEquals(
      diffedDf.select(
        col(s"t1${colJoin}id"),
        col(s"t2${colJoin}id"),
        col(equalityColName(s"t1${colJoin}double1", s"t2${colJoin}double1")),
        col(equalityColName(s"t1${colJoin}decimal1", s"t2${colJoin}decimal1"))
      ),
      numericThresholdTestDf.select(
        col("id").as(s"t1${colJoin}id"),
        col("id").as(s"t2${colJoin}id"),
        lit(equalStr).as(equalityColName(s"t1${colJoin}double1", s"t2${colJoin}double1")),
        lit(equalStr).as(equalityColName(s"t1${colJoin}decimal1", s"t2${colJoin}decimal1"))
      )
    )
  }

  it should "return not equals for both the perturbed data set within the threshold" taggedAs generateDiffTag in {
    val joinCols = Seq("id")
    val diffedDf = diffCreator.createDiff(numericThresholdTestDf, "t1", numericThresholdTestDfPerturbed, "t2", TnDiffParams(
      TnDiffInput(joinCols, Seq("double1", "decimal1")),
      TnDiffInput(joinCols, Seq("double1", "decimal1"))), 1e-7, true)

    dfEquals(
      diffedDf.select(
        col(s"t1${colJoin}id"),
        col(s"t2${colJoin}id"),
        col(equalityColName(s"t1${colJoin}double1", s"t2${colJoin}double1")),
        col(equalityColName(s"t1${colJoin}decimal1", s"t2${colJoin}decimal1"))
      ),
      numericThresholdTestDf.select(
        col("id").as(s"t1${colJoin}id"),
        col("id").as(s"t2${colJoin}id"),
        lit(diffStr).as(equalityColName(s"t1${colJoin}double1", s"t2${colJoin}double1")),
        lit(diffStr).as(equalityColName(s"t1${colJoin}decimal1", s"t2${colJoin}decimal1"))
      )
    )
  }

  it should "be able to compare nested data without crashing" taggedAs generateDiffTag in {
    val diffDf = diffCreator.createDiff(otherEqualityDf, "t1", otherEqualityDf, "t2",
      TnDiffParams(TnDiffInput(Seq("id"), Seq("struct1.int")), TnDiffInput(Seq("id"), Seq("struct1.int"))), filterEqualRows = true)
    dfEquals(diffDf, twoPointOtherDf.where(lit(false)).select(
      col("id").as(s"t1${colJoin}id"), col("id").as(s"t2${colJoin}id"),
      col("struct1.int").as(s"t1${colJoin}struct1_int"), col("struct1.int").as(s"t2${colJoin}struct1_int"),
      lit(0).as(minusColName(s"t1${colJoin}struct1_int", s"t2${colJoin}struct1_int")),
      lit(equalStr).as(equalityColName(s"t1${colJoin}struct1_int", s"t2${colJoin}struct1_int"))))
  }
}