from csvmod import *


class DemoController1(Controller):
    """
    You can document this controller here, describing required
    files, when to use this controller or anything else you
    can think of.
    """
    settings = dict(
        converter={
            "Price": comma_decimal,
            "Shipping": comma_decimal
        },
        format=dict(
            delimiter=";"
        ),
        joins=(
            JoinCSV(
                name="test",
                file="example2.csv",
                local="AuctionID",
                remote="auction_id",
                format=dict(
                    delimiter="|",
                ),
                joins=(
                    JoinCSV(
                        name="nested",
                        file="example3.csv",
                        local="listing_id",
                        remote="ID",
                        format=dict(
                            delimiter="|",
                        ),
                    )
                ),
            )
        ),
    )
    output = dict(
        format=dict(
            delimiter="\t",
        ),
        fields=("AuctionID", "Dispatch", "Service", "ListingID", "ListingName")
    )

    row = 0

    def handle(self, data: CSVRow):
        self.row += 1

        if not data["ItemNo"].startswith("u0") or self.row > 50:
            return False

        if data["Service"] not in ("7723", "7710", "7730"):
            data["Service"] = "7723"

        if data["Service"] == "7723":
            data["Dispatch"] = 1

        if data["Price"] >= 40 and data["Service"] == "7723":
            data["Price"] = data["Price"] + data["Shipping"]
            data["Shipping"] = 0
            data["Connect2ItemPrice"] = 0

        test = data.join("test")

        if test:
            data["ListingID"] = test["listing_id"]
            data["ListingName"] = test.join("nested", "Name")


class DemoController2(Controller):
    """
    A controller with some more settings
    """
    options = dict(
        delimiter="|",
        converter={
            "ArticleNo": int,
            "DispatchTime": int,
            "Price": comma_decimal
        },
        fields=("Price", "Shipping", "DispatchTime", "ArticleNo")
    ),
    output = dict(
        formatter={
            "Price": comma_decimal_formatter,
        },
        format=dict(delimiter=";")
    ),

    def handle(self, data):
        data["Shipping"] = data["ArticleNo"] + data["DispatchTime"]
        data["ArticleNo"] = 1234
