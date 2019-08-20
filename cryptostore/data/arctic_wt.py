import pandas as pd
from collections import OrderedDict as od


def wide_book(data):
    """
    Function used to store l2_book data in an alternative way.
    Not default and must be enabled in the config.yaml under wide_tables: true

    data: list of dictionaries 
    
    Example with book_depth = 3:

    data = [
    {
        "ask": {"11371.73": 0.80941112, "11371.74": 0.2, "11372.28": 4.23300088},
        "bid": {"11368.35": 1.59830075, "11368.37": 0.42152563, "11368.38": 0.60871406},
        "delta": False,
        "timestamp": 1565551972.7636518,
    },
    {
        "ask": {"11371.71": 1.55941112, "11371.74": 0.2, "11372.28": 4.23300088},
        "bid": {"11368.35": 1.59830072, "11368.37": 0.42152563, "11368.39": 0.60871406},
        "delta": False,
        "timestamp": 1565551972.764616,
    },
    ...]

    Notice that both [ask] and [bid] keys are stored in increasing value.
    We must reverse the order of the bids, so when zipped together we get the
    different book levels in the correct order.
    
    Example: highest_bid - lowest_ask, 
             second_highest_bid - second_lowest_ask,
             ...
    """
    odict_list = []
    odict = od({})

    for dic in data:
        odict.update({"timestamp": dic["timestamp"]})
        for index, i in enumerate(
            zip(dic["ask"].items(), sorted(dic["bid"].items(), reverse=True))
        ):
            odict.update({f"asks[{index}].price": f"{i[0][0]}"})
            odict.update({f"asks[{index}].size": f"{i[0][1]}"})
            odict.update({f"bids[{index}].price": f"{i[1][0]}"})
            odict.update({f"bids[{index}].size": f"{i[1][1]}"})
        odict_list.append(odict.copy())

    df = pd.DataFrame(odict_list)
    df["date"] = pd.to_datetime(df["timestamp"], unit="s")
    df.set_index(df["date"], inplace=True)
    df.drop(["date", "timestamp"], axis=1, inplace=True)
    df = df.apply(pd.to_numeric)
    return df
