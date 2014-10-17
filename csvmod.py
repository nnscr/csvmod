#!/usr/bin/python
import csv


def comma_decimal(val):
    return float(val.replace(",", "."))


def comma_decimal_formatter(val):
    return str(val).replace(".", ",")


class CSVError(Exception):
    pass


class CSVHeaderError(CSVError):
    def __init__(self, expected, actual):
        CSVError.__init__(self, "Invalid header")
        self.expected = expected
        self.actual = actual


class CSVFieldError(CSVError):
    def __init__(self, field, file=None):
        if file is None:
            CSVError.__init__(self, "Unknown field '%s'" % field)
        else:
            CSVError.__init__(self, "Unknown field '%s' in file '%s'" % (field, file))


class CSVMod(object):
    def __init__(self, controller):
        self.controller = controller

    def start(self):
        reader = self.controller.reader
        reader.begin()

        writer = self.controller.writer
        writer.begin()
        writer.writeheader()

        for data in reader:
            row = reader.create_row(data)

            update = self.controller.handle(row)
            self.controller.post_progress(row)

            if update is None:
                update = row.is_changed

            if update:
                writer.write(row.fields)

        self.controller.finish()


class CSVRow(object):
    """
    :type joins: dict
    """
    def __init__(self, fields, joins, aliases: dict, file_name=None):
        self.fields = fields
        self.origin = dict(fields)
        self.joins = joins
        self.aliases = aliases
        self.file_name = file_name

    def __getitem__(self, item):
        return self.fields[self._get_field_name(item, True)]

    def __setitem__(self, key, value):
        getattr(self, "fields")[self._get_field_name(key, False)] = value

    def __repr__(self):
        return str(self.fields)

    def _get_field_name(self, key, strict=True) -> str:
        if key in self.fields:
            return key

        if key in self.aliases:
            return self.aliases[key]

        if strict:
            raise CSVFieldError(key, file=self.file_name)
        else:
            return key

    @property
    def is_changed(self) -> bool:
        return self.origin != self.fields

    def join(self, name, field=None):
        """
        :rtype: CSVRow
        """
        try:
            join = self.joins[name]
        except KeyError:
            raise CSVError("Unknown join '%s'" % name)

        joint = join.auto_join(self)

        if joint is None:
            return None

        if field is not None:
            return joint[field]

        return joint

    def has_join(self, name) -> bool:
        return name in self.joins.keys()


class CSVFile(object):
    def __init__(self, **kwargs):
        self.file_name = kwargs.pop("file")
        self.file_handle = None
        self.format = dict(delimiter=";", quotechar='"')
        self.encoding = kwargs.pop("encoding", "utf-8")
        self.fields = kwargs.pop("fields", None)
        self.aliases = kwargs.pop("aliases", dict())
        self.converter = kwargs.pop("converter", dict())
        self.base_csv = None
        self.name = kwargs.pop("name", None)

        if "format" in kwargs:
            self.format.update(kwargs.pop("format"))

        if len(kwargs) > 0:
            raise KeyError("Invalid option: %s" % ", ".join(kwargs.keys()))

    def begin(self):
        pass

    def end(self):
        self.file_handle.close()

    def _reduce_fields(self, row: dict) -> dict:
        return {k: v for k, v in row.items() if k in self.fields}


class CSVReadFile(CSVFile):
    def __init__(self, **kwargs):
        self.joins = kwargs.pop("joins", list())

        if isinstance(self.joins, JoinCSV):
            self.joins = (self.joins, )

        joins = dict()
        for join in self.joins:
            joins[join.name] = join

        self.joins = joins

        if "name" not in kwargs:
            kwargs["name"] = "main"

        super().__init__(**kwargs)

    def __iter__(self):
        return self.base_csv

    def check_header(self, header):
        if self.fields is None:
            return True

        header = list(header)
        fields = list(self.fields)

        for field in fields:
            if field not in header:
                raise CSVHeaderError(field, header)

    def create_row(self, data) -> CSVRow:
        for field, converter in self.converter.items():
            data[field] = converter(data[field])

        return CSVRow(self._reduce_fields(data), self.joins, self.aliases, self.name)

    @property
    def reader(self) -> csv.DictReader:
        if not self.base_csv:
            self.file_handle = open(self.file_name, "r", encoding=self.encoding)
            self.base_csv = csv.DictReader(self.file_handle, **self.format)

        return self.base_csv

    def begin(self):
        if self.fields is None:
            self.fields = self.reader.fieldnames
        else:
            fields = list()
            for field in self.fields:
                if field in self.aliases:
                    fields.append(self.aliases[field])
                else:
                    fields.append(field)

            self.fields = fields

            self.check_header(self.reader.fieldnames)

        for join in self.joins.values():
            join.begin()

    def end(self):
        super().end()
        for join in self.joins.values():
            join.end()


class CSVWriteFile(CSVFile):
    def __init__(self, **kwargs):
        self.formatter = kwargs.pop("formatter", dict())

        if "name" not in kwargs:
            kwargs["name"] = "target"

        super().__init__(**kwargs)

    @property
    def writer(self) -> csv.DictWriter:
        if not self.base_csv:
            self.file_handle = open(self.file_name, "w", encoding=self.encoding)
            self.base_csv = csv.DictWriter(self.file_handle, self.fields, **self.format)

        return self.base_csv

    def write(self, data):
        for field, formatter in self.formatter.items():
            data[field] = formatter(data[field])

        self.writer.writerow(self._reduce_fields(data))

    def writeheader(self):
        self.writer.writeheader()


class JoinCSV(CSVReadFile):
    def __init__(self, **kwargs):
        self.local_field = kwargs.pop("local")
        self.join_field = kwargs.pop("remote")

        self.cache_enabled = kwargs.pop("cache", True)
        self.cache = dict()

        if "name" not in kwargs:
            kwargs["name"] = kwargs["file"]

        super().__init__(**kwargs)

    def get_row(self, criteria) -> CSVRow:
        if self.cache_enabled:
            f = self.get_row_cached
        else:
            f = self.get_row_uncached

        data = f(criteria)

        return data

    def get_row_uncached(self, criteria) -> dict:
        for row in self.reader:
            if self._is_match(row, criteria):
                return self.create_row(row)

    def get_row_cached(self, criteria) -> dict:
        if criteria in self.cache:
            return self.cache[criteria]

        for row in self.reader:
            r = self.create_row(row)
            self.cache[row[self.join_field]] = r

            if self._is_match(r, criteria):
                return r

    def auto_join(self, row: CSVRow) -> CSVRow:
        criteria = row[self.local_field]
        return self.get_row(criteria)

    def _is_match(self, row, criteria) -> bool:
        return row[self.join_field] == criteria


class Statistics(object):
    class Counter(object):
        def __init__(self, allow_negative=True):
            self.slots = dict()
            self.allow_negative = allow_negative

        def plus(self, slot, n=1):
            try:
                self.slots[slot] += n
            except KeyError:
                self.slots[slot] = n

            if not self.allow_negative and self[slot] < 0:
                self.slots[slot] = 0

        def minus(self, slot, n=1):
            self.plus(slot, -n)

        def __getitem__(self, item):
            return self.slots.get(item, 0)

    def __init__(self):
        self.changes = {}
        self.rows = 0

    def process(self, data):
        changed = False
        for field in data.origin.keys():
            if data.origin[field] != data[field]:
                changed = True
                self._incr("changes", field)

        if changed:
            self.rows += 1

    def finish(self):
        import operator
        print("Finished, modified %d rows." % self.rows)
        s = sorted(self.changes.items(), key=operator.itemgetter(1))

        for field, changes in s:
            print("%6d %s" % (changes, field))

    def _incr(self, prop, index, n=1):
        try:
            self.__dict__[prop][index] += n
        except KeyError:
            self.__dict__[prop][index] = n


class Controller(object):
    statistics = [Statistics()]
    settings = dict()
    output = dict()

    def __init__(self, input_file=None, output_file=None):
        if input_file is not None:
            self.settings["file"] = input_file

        if output_file is not None:
            self.output["file"] = output_file

        self._reader = None
        self._writer = None

    def handle(self, data):
        pass

    @property
    def reader(self) -> CSVReadFile:
        if not self._reader:
            self._reader = CSVReadFile(**self.settings)

        return self._reader

    @property
    def writer(self) -> CSVWriteFile:
        if not self._writer:
            opts = dict(self.output)
            if opts.get("fields") is None:
                opts["fields"] = self.reader.fields

            self._writer = CSVWriteFile(**opts)

        return self._writer

    def post_progress(self, data):
        for stat in self.statistics:
            stat.process(data)

    def finish(self):
        self._reader.end()

        for stat in self.statistics:
            stat.finish()


if __name__ == "__main__":
    from sys import argv, path
    from os import getcwd

    def import_controller(name):
        components = name.split('.')
        module = __import__(components[0])
        for comp in components[1:]:
            module = getattr(module, comp)

        return module

    if len(argv) != 4:
        print("Usage: %s <controller> <input> <output>" % argv[0])
        exit(1)

    controller_name = argv[1]
    read_file = argv[2]
    write_file = argv[3]

    path.append(getcwd())
    chosen_controller = import_controller(controller_name)

    mod = CSVMod(chosen_controller(read_file, write_file))

    try:
        mod.start()
    except CSVHeaderError as e:
        print("Unexpected header detected.")
        print(e.expected)
        print(e.actual)
