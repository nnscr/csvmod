from csvmod import Controller, price


class DemoController(Controller):
    converters = {"Price": price, "Shipping": price}

    def handle(self, origin, row):
        if not row["ItemNo"].startswith("u0"):
            return

        if row["Service"] not in ("7723", "7710", "7730"):
            row["Service"] = "7723"

        if row["Service"] == "7723":
            row["Dispatch"] = 1

        if row["Price"] >= 40 and row["Service"] == "7723":
            row["Price"] = row["Price"] + row["Shipping"]
            row["Shipping"] = 0
            row["Connect2ItemPrice"] = 0


class TestController(Controller):
    """
    Write something about this csv modifier!
    """
    delimiter = "|"
    converter = {"ArticleNo": int, "DispatchTime": int}
    fields = ("Price", "Shipping", "DispatchTime", "ArticleNo")

    def handle(self, origin, row):
        print(origin)

        row["Shipping"] = row["ArticleNo"] + row["DispatchTime"]
