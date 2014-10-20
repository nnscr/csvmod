import pickle
from unittest import TestCase
from csvmod import *
import unittest.main
from unittest import mock


class JoinCSVMock(object):
    test_row = CSVRow({"jfoo": "jbar"}, dict(), dict())
    auto_join = mock.Mock(return_value=test_row)

    def __init__(self, name=None):
        self.name = name


class TestCounter(TestCase):
    def test_plus(self):
        counter = Statistics.Counter()
        self.assertEqual(counter["foo"], 0)
        self.assertEqual(counter["bar"], 0)
        counter.plus("foo")
        self.assertEqual(counter["foo"], 1)
        self.assertEqual(counter["bar"], 0)
        counter.plus("bar", 4)
        self.assertEqual(counter["foo"], 1)
        self.assertEqual(counter["bar"], 4)

    def test_minus(self):
        counter = Statistics.Counter(allow_negative=True)
        counter.plus("foo", 10)
        counter.minus("foo")
        self.assertEqual(counter["foo"], 9)
        counter.minus("foo", 5)
        self.assertEqual(counter["foo"], 4)
        counter.minus("bar", 10)
        self.assertEqual(counter["bar"], -10)

        counter = Statistics.Counter(allow_negative=False)
        counter.plus("bar", 15)
        counter.minus("bar", 10)
        self.assertEqual(counter["bar"], 5)
        counter.minus("bar", 20)
        self.assertEqual(counter["bar"], 0)


class TestCSVRow(TestCase):
    f = {"foo": "bar", "bar": "foo"}

    def test__get_field_name(self):
        a = {"test": "bar"}
        row = CSVRow(dict(self.f), dict(), a)

        self.assertEqual("foo", row._get_field_name("foo"))
        self.assertEqual("bar", row._get_field_name("test"))
        self.assertEqual("bar", row._get_field_name("bar"))
        self.assertRaises(CSVFieldError, row._get_field_name, "baz")

    def test_item_accessor(self):
        row = CSVRow(dict(self.f), dict(), dict())
        self.assertEqual("bar", row["foo"])
        self.assertEqual("foo", row["bar"])
        row["foo"] = "baz"
        self.assertEqual("baz", row["foo"])

    def test_is_changed(self):
        row = CSVRow(dict(self.f), dict(), dict())
        self.assertEqual(False, row.is_changed)
        row["foo"] = "bar"
        self.assertEqual(False, row.is_changed)
        row["foo"] = "something else"
        self.assertEqual(True, row.is_changed)

    def test_has_join(self):
        row = CSVRow(dict(self.f), {"foo": None, "bar": None}, dict())
        self.assertEqual(True, row.has_join("foo"))
        self.assertEqual(True, row.has_join("bar"))
        self.assertEqual(False, row.has_join("baz"))

    def test_join(self):
        jmock = JoinCSVMock()

        row = CSVRow(dict(self.f), {"foo": jmock}, dict())
        self.assertRaises(CSVError, row.join, "bar")
        self.assertEqual(jmock.test_row, row.join("foo"))
        self.assertEqual("jbar", row.join("foo", "jfoo"))


class TestCSVFile(TestCase):
    def test___init__(self):
        self.assertRaises(KeyError, CSVFile)
        self.assertIsInstance(CSVFile(file="foo"), CSVFile)
        self.assertRaises(KeyError, CSVFile, file="foo", inexistent="bar")

    def test__reduce_fields(self):
        c = CSVFile(file="foo", fields=("foo", "bar"))
        self.assertEqual({"foo": "foo", "bar": "bar"}, c._reduce_fields({"foo": "foo", "bar": "bar", "baz": "baz"}))
        self.assertEqual({"foo": "foo"}, c._reduce_fields({"foo": "foo", "baz": "baz"}))

    def test_fields(self):
        c = CSVFile(file="foo", aliases={"f": "foo", "b": "bar"})
        self.assertListEqual([], c.fields)
        c.fields = ("f", "bar")
        self.assertListEqual(["foo", "bar"], c.fields)
        c.fields = ("foo", "bar")
        self.assertListEqual(["foo", "bar"], c.fields)
        c.fields = ("f", "bar")
        self.assertListEqual(["foo", "bar"], c.fields)


class TestCSVReadFile(TestCase):
    def test_set_joins(self):
        foo, bar = JoinCSVMock("foo"), JoinCSVMock("bar")
        c = CSVReadFile(file="")

        self.assertEqual(dict(), c.joins)
        c.joins = foo
        self.assertEqual({"foo": foo}, c.joins)
        c.joins = [foo, bar]
        self.assertEqual({"foo": foo, "bar": bar}, c.joins)
        c.joins = (foo, bar)
        self.assertEqual({"foo": foo, "bar": bar}, c.joins)

    def test_check_header(self):
        c = CSVReadFile(file="")
        self.assertEqual(True, c.check_header(("foo", "bar", "baz")))
        c.fields = ("foo", "bar")
        self.assertEqual(True, c.check_header(("foo", "bar", "baz")))
        self.assertRaises(CSVHeaderError, c.check_header, ("a", "b", "c"))
        self.assertRaises(CSVHeaderError, c.check_header, ("foo", ))

    def test_create_row(self):
        c = CSVReadFile(file="", converter={"foo": int, "bar": float}, fields=("foo", "bar", "baz"))
        r = c.create_row({"foo": "123", "bar": "12.34", "baz": "321"})
        self.assertIsInstance(r, CSVRow)
        self.assertEqual(r["foo"], 123)
        self.assertEqual(r["bar"], 12.34)
        self.assertEqual(r["baz"], "321")


class TestCSVWriteFile(TestCase):
    def test_write(self):
        data = {"foo": "bar", "bar": "foo"}

        writerow = mock.Mock()
        c = CSVWriteFile(file="")
        c.base_csv = mock.Mock(spec=csv.DictWriter)
        c.base_csv.writerow = writerow
        c.fields = ["foo", "bar"]

        c.write(data)
        writerow.assert_called_once_with(data)
        writerow.reset_mock()

        c.formatter = {"foo": lambda s: s[::-1]}
        c.write(data)
        writerow.assert_called_once_with({"foo": "rab", "bar": "foo"})
        writerow.reset_mock()
        c.formatter = {}

        c.fields = ["bar"]
        c.write(data)
        writerow.assert_called_once_with({"bar": "foo"})
        writerow.reset_mock()

        c.aliases = {"foo": "test1", "bar": "test2"}
        c.fields = ["test1", "test2"]
        c.write(data)
        writerow.assert_called_once_with({"test1": "bar", "test2": "foo"})
        writerow.reset_mock()


class TestJoinCSV(TestCase):
    def test_get_row(self):
        c = JoinCSV(local="", remote="", file="")
        c.base_csv = mock.Mock(spec=csv.DictReader)


if __name__ == "__main__":
    unittest.main()
