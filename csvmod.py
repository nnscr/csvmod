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


class CSVMod(object):
    def __init__(self, controller):
        self.controller = controller
        print(self.controller.options)

    def start(self, input_file, output_file):
        reader = csv.DictReader(input_file, **self.controller.get_reader_options())
        writer = csv.DictWriter(output_file, reader.fieldnames, **self.controller.get_writer_options())

        self.controller.check_header(reader.fieldnames)
        joins = self.controller.import_joins()

        writer.writeheader()

        for data in reader:
            for field, converter in self.controller.converter.items():
                data[field] = converter(data[field])

            row = CSVRow(data, joins, self.controller.get_aliases())
            self.controller.handle(row)
            self.controller.post_progress(row)

            if row.is_changed:
                raw = row.fields
                for field, formatter in self.controller.formatter.items():
                    raw[field] = formatter(raw[field])

                writer.writerow(raw)

        self.controller.finish()


class CSVRow(object):
    def __init__(self, fields, joins, aliases: dict):
        self.fields = fields
        self.origin = dict(fields)
        self.joins = joins
        self.aliases = aliases

    def __getitem__(self, item):
        if item in self.fields:
            return self.fields[item]

        if item in self.aliases:
            return self.fields[self.aliases[item]]

    def __setitem__(self, key, value):
        getattr(self, "fields")[key] = value

    @property
    def is_changed(self):
        return self.origin != self.fields

    def join(self, name, field=None):
        try:
            join = self.joins[name]
        except KeyError:
            raise CSVError("Unknown join '%s'" % name)

        joint = join.auto_join(self)

        if field is not None and joint is not None:
            return joint[field]

        return joint

    def has_join(self, name):
        return name in self.joins.keys()


class CSVFile(object):
    def __init__(self, **kwargs):
        self.reader = None
        self.file_name = kwargs.pop("file")
        self.file_handle = None
        self.options = dict(delimiter=";", quotechar='"')
        self.encoding = kwargs.pop("encoding", "utf-8")
        self.aliases = kwargs.pop("aliases", dict())
        self.fields = kwargs.pop("fields", None)

        if "options" in kwargs:
            self.options.update(kwargs.pop("options"))

        if len(kwargs) > 0:
            raise TypeError("Invalid option: %s" % ", ".join(kwargs.keys()))

    def get_reader(self):
        if not self.reader:
            self.file_handle = open(self.file_name, "r", encoding=self.encoding)
            self.reader = csv.DictReader(self.file_handle, **self.options)

        return self.reader

    def begin(self):
        # important: also get the reader if fields are passed to open the file
        reader = self.get_reader()

        if self.fields is None:
            self.fields = reader.fieldnames
        else:
            fields = list()
            for field in self.fields:
                if field in self.aliases:
                    fields.append(self.aliases[field])
                else:
                    fields.append(field)
            self.fields = fields

    def end(self):
        self.file_handle.close()

    def _reduce_fields(self, row):
        return {k: v for k, v in row.items() if k in self.fields}


class JoinCSV(CSVFile):
    def __init__(self, **kwargs):
        self.local_field = kwargs.pop("local")
        self.join_field = kwargs.pop("remote")
        self.name = kwargs.pop("name", kwargs["file"])

        self.cache_enabled = kwargs.pop("cache", True)
        self.cache = dict()

        super().__init__(**kwargs)

    def get_row(self, criteria):
        if self.cache:
            f = self.get_row_cached
        else:
            f = self.get_row_uncached

        data = f(criteria)

        if data is None:
            return None

        return CSVRow(data, None, self.aliases)

    def get_row_uncached(self, criteria):
        for row in self.get_reader():
            if self._is_match(row, criteria):
                return self._reduce_fields(row)

        return None

    def get_row_cached(self, criteria):
        if criteria in self.cache:
            return self.cache[criteria]

        for row in self.get_reader():
            reduced = self._reduce_fields(row)
            self.cache[row[self.join_field]] = reduced

            if self._is_match(row, criteria):
                return reduced

    def _is_match(self, row, criteria):
        return row[self.join_field] == criteria

    def auto_join(self, row):
        criteria = row[self.local_field]
        return self.get_row(criteria)


class Statistics(object):
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
        print("Finished, modified %d rows." % self.rows)
        for field, changes in self.changes.items():
            print("%5d %s" % (changes, field))

    def _incr(self, prop, index, n=1):
        try:
            self.__dict__[prop][index] += n
        except KeyError:
            self.__dict__[prop][index] = n


class Controller(object):
    delimiter = ";"
    quotechar = '"'
    converter = dict()
    formatter = dict()
    fields = None
    statistics = [Statistics()]
    joins = ()
    aliases = dict()
    csv_options = dict()

    @property
    def options(self):
        return type(self).__dict__

    def handle(self, data):
        pass

    def check_header(self, header):
        if self.fields is None:
            return True

        header = list(header)
        fields = list(self.fields)

        if header != fields:
            raise CSVHeaderError(fields, header)

    def get_reader_options(self):
        try:
            return self.reader_options
        except AttributeError:
            return dict(
                delimiter=self.delimiter,
                quotechar=self.quotechar,
            )

    def get_writer_options(self):
        try:
            return self.writer_options
        except AttributeError:
            return self.get_reader_options()

    def post_progress(self, data):
        for stat in self.statistics:
            stat.process(data)

    def finish(self):
        for stat in self.statistics:
            stat.finish()

        for join in self.joins:
            join.end()

    def import_joins(self):
        result = dict()

        if isinstance(self.joins, JoinCSV):
            self.joins = (self.joins, )

        for join in self.joins:
            join.begin()
            result[join.name] = join

        return result

    def get_aliases(self):
        return self.aliases


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

    mod = CSVMod(chosen_controller())

    with open(read_file, "r") as input_handle, open(write_file, "w") as output_handle:
        try:
            mod.start(input_handle, output_handle)
        except CSVHeaderError as e:
            print("Unexpected header detected.")
            print(e.expected)
            print(e.actual)
