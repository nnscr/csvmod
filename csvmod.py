import csv


def price(val):
    return float(val.replace(",", "."))


class CSVError(Exception):
    @staticmethod
    def invalid_header(expected, actual):
        error = CSVError("Invalid header")
        error.expected = expected
        error.actual = actual

        return error


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

            if origin != row:
                writer.writerow(row)

    def apply_change(self, field, new_value):
        pass


class Controller(object):
    delimiter = ";"
    quotechar = '"'
    converter = dict()
    fields = None

    def handle(self, origin, row):
        pass

    def check_header(self, header):
        if self.fields is None:
            return True

        header = list(header)
        fields = list(self.fields)

        if header != fields:
            raise CSVError.invalid_header(fields, header)

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
        except CSVError as e:
            print(e)
        except Exception as e:
            print("Unexpected header detected.")
            print(e.expected)
            print(e.actual)
