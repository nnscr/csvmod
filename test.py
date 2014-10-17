import pickle
from unittest import TestCase
from csvmod import *
import unittest.main
from unittest import mock


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
        jrow = CSVRow({"jfoo": "jbar"}, dict(), dict())

        class JoinCSVMock(object):
            auto_join = mock.Mock(return_value=jrow)

        jmock = JoinCSVMock()

        row = CSVRow(dict(self.f), {"foo": jmock}, dict())
        self.assertRaises(CSVError, row.join, "bar")
        self.assertEqual(jrow, row.join("foo"))
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


if __name__ == "__main__":
    unittest.main()
