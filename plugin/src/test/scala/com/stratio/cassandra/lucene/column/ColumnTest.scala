/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.lucene.column

import java.text.SimpleDateFormat
import java.util.Date

import com.stratio.cassandra.lucene.column.Column._
import com.stratio.cassandra.lucene.BaseScalaTest
import com.stratio.cassandra.lucene.BaseScalaTest._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/** Tests for [[Column]].
  *
  * @author Andres de la Pena `adelapena@stratio.com`
  */
@RunWith(classOf[JUnitRunner])
class ColumnTest extends BaseScalaTest {

  test("set default attributes") {
    val column = new Column("cell")
    column.cell shouldBe "cell"
    column.mapper shouldBe "cell"
    column.field shouldBe "cell"
    column.value shouldBe None
  }

  test("set all attributes") {
    val column = new Column("cell")
      .withUDTName("u1")
      .withUDTName("u2")
      .withMapName("m1")
      .withMapName("m2")
      .withValue(5)
    column.cell shouldBe "cell"
    column.mapper shouldBe "cell.u1.u2"
    column.field shouldBe "cell.u1.u2$m1$m2"
    column.value shouldBe Some(5)
  }

  test("fieldName") {
    new Column("c").fieldName("f") shouldBe "f"
    new Column("c").withUDTName("u").fieldName("f") shouldBe "f"
    new Column("c").withMapName("m").fieldName("f") shouldBe "f$m"
    new Column("c").withUDTName("u").withMapName("m").fieldName("f") shouldBe "f$m"
  }

  test("parse cell name") {
    Column.parseCellName("c") shouldBe "c"
    Column.parseCellName("c.u") shouldBe "c"
    Column.parseCellName("c$m") shouldBe "c"
    Column.parseCellName("c.u$m") shouldBe "c"
    Column.parseCellName("c.u1.u2$m1$m2") shouldBe "c"
  }

  test("parse mapper name") {
    Column.parseMapperName("c") shouldBe "c"
    Column.parseMapperName("c.u") shouldBe "c.u"
    Column.parseMapperName("c$m") shouldBe "c"
    Column.parseMapperName("c.u$m") shouldBe "c.u"
    Column.parseMapperName("c.u1.u2$m1$m2") shouldBe "c.u1.u2"
  }

  test("parse udt names") {
    Column.parseUdtNames("c") shouldBe Nil
    Column.parseUdtNames("c.u") shouldBe List("u")
    Column.parseUdtNames("c$m") shouldBe Nil
    Column.parseUdtNames("c.u$m") shouldBe List("u")
    Column.parseUdtNames("c.u1.u2$m1$m2") shouldBe List("u1", "u2")
  }

  test("add column") {
    new Column("a").at(new Column("b")) shouldBe Columns.of(new Column("a"), new Column("b"))
    new Column("b").at(new Column("a")) shouldBe Columns.of(new Column("b"), new Column("a"))
  }

  test("add columns") {
    new Column("a").at(Columns.of(new Column("b"), new Column("c"))) shouldBe
      Columns.of(new Column("a"), new Column("b"), new Column("c"))
  }

  test("toString with default attributes") {
    new Column("cell").toString shouldBe
      s"Column{cell=cell, field=cell, value=None}"
  }

  test("toString with all attributes") {
    new Column("cell")
      .withUDTName("u1")
      .withUDTName("u2")
      .withMapName("m1")
      .withMapName("m2")
      .withValue(5)
      .toString shouldBe
      "Column{cell=cell, field=cell.u1.u2$m1$m2, value=Some(5)}"
  }

  test("compose with basic types") {
    compose(ascii.decompose("aB"), ascii) shouldBe "aB"
    compose(utf8.decompose("aB"), utf8) shouldBe "aB"
    compose(byte.decompose(2.toByte), byte) shouldBe 2.toByte
    compose(short.decompose(2.toShort), short) shouldBe 2.toShort
    compose(int32.decompose(2), int32) shouldBe 2
    compose(long.decompose(2l), long) shouldBe 2l
    compose(float.decompose(2.1f), float) shouldBe 2.1f
    compose(double.decompose(2.1d), double) shouldBe 2.1d
  }

  test("compose with SimpleDateType") {
    val expected: Date = new SimpleDateFormat("yyyy-MM-ddZ").parse("1982-11-27+0000")
    val bb = date.fromTimeInMillis(expected.getTime)
    val actual = compose(bb, date)
    actual shouldBe a[Date]
    actual shouldBe expected
  }

  test("with composed value") {
    new Column("c").withValue(ascii.decompose("aB"), ascii) shouldBe new Column("c").withValue("aB")
    new Column("c").withValue(utf8.decompose("aB"), utf8) shouldBe new Column("c").withValue("aB")
    new Column("c").withValue(byte.decompose(2.toByte), byte) shouldBe new Column("c").withValue(2.toByte)
    new Column("c").withValue(short.decompose(2.toShort), short) shouldBe new Column("c").withValue(2.toShort)
    new Column("c").withValue(int32.decompose(2), int32) shouldBe new Column("c").withValue(2)
    new Column("c").withValue(long.decompose(2l), long) shouldBe new Column("c").withValue(2l)
    new Column("c").withValue(float.decompose(2.1f), float) shouldBe new Column("c").withValue(2.1f)
    new Column("c").withValue(double.decompose(2.1d), double) shouldBe new Column("c").withValue(2.1d)
  }

  test("with value") {
    new Column("c").withValue(null) shouldBe new Column("c")
    new Column("c").withValue(3).withValue(null) shouldBe new Column("c")
    new Column("c").withValue(3).withValue(4) shouldBe new Column("c").withValue(4)
    new Column("c").withValue(null).withValue(3) shouldBe new Column("c").withValue(3)
    new Column("c").withValue(3).withValue(3) shouldBe new Column("c").withValue(3)
  }
}
