#!/usr/bin/env python3
# coding: utf-8
import requests
import pandas as pd
import json
from config import array, fields
import datetime

history = []


def req_data(obj):
    url = f"http://127.0.0.1:8000/ohlcv?name={obj['name']}&frequency={obj['frequency']}&count={obj['count']}&refresh={obj['refresh']}"

    r = requests.get(url)
    jsonData = json.loads(r.text)

    data = pd.DataFrame(
        {
            "time": jsonData[0],
            "open": jsonData[1],
            "high": jsonData[2],
            "low": jsonData[3],
            "close": jsonData[4],
        }
    )
    data["date"] = (
        pd.to_datetime(data.time, unit="s")
        .dt.tz_localize("UTC")
        .dt.tz_convert("Asia/Shanghai")
    )
    data["date"] = data["date"].astype(str)
    return data


def gen_2k(data):
    close = data["close"]
    high = data["high"]
    low = data["low"]
    kLineHigh2 = high.shift(+1)
    kLineLow2 = low.shift(+1)
    kLineHigh3 = high.shift(+2)
    kLineLow3 = low.shift(+2)

    closeGt1 = close > kLineHigh2
    closeGt2 = close > kLineHigh3
    closeGt = closeGt1 & closeGt2
    # closeGt = closeGt.shift(-2)
    # lastGt = closeGt.loc[closeGt.index.stop - 1]
    data["closeGt"] = closeGt
    closeGt2 = closeGt.shift(+1)
    closeGtDiff = closeGt == closeGt2
    data["closeGtDiff"] = closeGtDiff
    closeLt1 = close < kLineLow2
    closeLt2 = close < kLineLow3
    closeLt = closeLt1 & closeLt2
    # closeLt = closeLt.shift(2)
    # lastLt = closeLt.loc[closeLt.index.stop - 1]
    data["closeLt"] = closeLt
    closeLt2 = closeLt.shift(+1)
    closeLtDiff = closeLt == closeLt2
    data["closeLtDiff"] = closeLtDiff


def send_bot(lastData):
    requests.post(
        "http://127.0.0.1:8000/send",
        data=lastData.to_json(),
    )


def find_value(dict, keyArr, number=0):
    if len(keyArr) <= number:
        if number == 0:
            return None
        else:
            return dict

    k = keyArr[number]
    if k not in dict:
        return None
    value = dict[k]
    if isinstance(value, type(dict)):
        return find_value(value, keyArr, number + 1)
    return value


def work_time():
    time_array = [["9:00", "11:00"], ["13:30", "15:00"], ["21:00", "23:00"]]
    for i in time_array:
        start = datetime.datetime.strptime(
            str(datetime.datetime.now().date()) + i[0], "%Y-%m-%d%H:%M"
        )
        end = datetime.datetime.strptime(
            str(datetime.datetime.now().date()) + i[1], "%Y-%m-%d%H:%M"
        )
        now_time = datetime.datetime.now()
        if now_time >= start and now_time <= end:
            print("now work time...", now_time)
            return True
        print("not work time...", now_time)
        return False


async def task(obj):
    while 1:
        print("run:", obj)
        if work_time():
            data = req_data(obj)
            gen_2k(data)
            last = data.loc[data.index.stop - 1].copy()
            if (last["closeGt"] and not last["closeGtDiff"]) | (
                last["closeLt"] and not last["closeLtDiff"]
            ):
                last["level"] = obj["frequency"]
                last["name"] = obj["name"]
                last = last.reindex(fields)
                last.drop(["closeGtDiff"], inplace=True)
                last.drop(["closeLtDiff"], inplace=True)

                h = f"{last['name']} {last['level']} {last['date']}"
                if h not in history:
                    send_bot(last)
                    history.append(h)
                    print("已发送", h)

        print(f"sleep(s): {obj['sleep']}")
        await asyncio.sleep(obj["sleep"])


async def main(array):
    await asyncio.gather(*(task(obj) for obj in array))


if __name__ == "__main__":
    import asyncio

    asyncio.run(main(array))

    # for obj in array:
    #         import pdb

    #         pdb.set_trace()
