import csv


def comma_decimal(val):
    return float(val.replace(",", "."))


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

    def start(self, input_file, output_file):
        converters = self.controller.converter

        reader = csv.DictReader(input_file, **self.controller.get_reader_options())
        writer = csv.DictWriter(output_file, reader.fieldnames, **self.controller.get_writer_options())

        self.controller.check_header(reader.fieldnames)

        writer.writeheader()

        for row in reader:
            for field, converter in converters.items():
                row[field] = converter(row[field])

            origin = row.copy()
            self.controller.handle(origin, row)
            self.controller.post_progress(origin, row)

            if origin != row:
                writer.writerow(row)

        self.controller.finish()


class Statistics(object):
    def __init__(self):
        self.changes = {}
        self.rows = 0

    def process(self, origin, row):
        changed = False
        for field in origin.keys():
            if origin[field] != row[field]:
                changed = True
                try:
                    self.changes[field] += 1
                except KeyError:
                    self.changes[field] = 1

        if changed:
            self.rows += 1

    def finish(self):
        print("Finished, modified %d rows." % self.rows)
        for field, changes in self.changes.items():
            print("%5d %s" % (changes, field))


class Controller(object):
    delimiter = ";"
    quotechar = '"'
    converter = dict()
    fields = None
    statistics = Statistics()

    def handle(self, origin, row):
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
            return type(self).reader_options
        except AttributeError:
            return dict(
                delimiter=self.delimiter,
                quotechar=self.quotechar,
            )

    def get_writer_options(self):
        try:
            return type(self).writer_options
        except AttributeError:
            return self.get_reader_options()

    def post_progress(self, origin, row):
        if self.statistics is not None:
            self.statistics.process(origin, row)

    def finish(self):
        if self.statistics is not None:
            self.statistics.finish()


if __name__ == "__main__":
    from sys import argv

    def import_controller(name):
        components = name.split('.')
        mod = __import__(components[0])
        for comp in components[1:]:
            mod = getattr(mod, comp)

        return mod

    if len(argv) != 4:
        print("Usage: %s <controller> <input> <output>" % argv[0])
        exit(1)

    controller_name = argv[1]
    read_file = argv[2]
    write_file = argv[3]

    chosen_controller = import_controller(controller_name)

    mod = CSVMod(chosen_controller())

    with open(read_file, "r") as input_handle, open(write_file, "w") as output_handle:
        try:
            mod.start(input_handle, output_handle)
        except CSVHeaderError as e:
            print("Unexpected header detected.")
            print(e.expected)
            print(e.actual)
