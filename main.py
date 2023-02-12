#!/usr/bin/env python3
# coding: utf-8
import requests
import pandas as pd
import json
from config import array, fields, current_array
import datetime

history = []
global_obj = []


def run_print(obj, data=None, mode=1):
    pr1 = f"name: {obj['name']} frequency: {obj['frequency']} count: {obj['count']} sleep: {obj['sleep']}"
    splitStart = obj["splitStart"] if "splitStart" in obj else None
    splitEnd = obj["splitEnd"] if "splitEnd" in obj else None
    pr_splitStart = "" if splitStart == None else f"splitStart {splitStart}"
    pr_splitEnd = "" if splitEnd == None else f"splitEnd {splitEnd}"
    if mode == "start":
        print(f"run: {pr1}")
    if mode == "real_data":
        print(
            f"真实数据:last -- {data.iloc[len(data) - 1]['date']} {pr_splitStart} {pr_splitEnd} "
        )
    if mode == "test_data":
        print(
            f"测试数据: last -- {data.iloc[len(data) - 1]['date']} {pr_splitStart} {pr_splitEnd}"
        )
        # print(data)


def add_global_obj(obj):
    global global_obj
    for index, i in enumerate(global_obj):
        if i["name"] == obj["name"] and i["frequency"] == obj["frequency"]:
            global_obj[index] = obj
            break
    else:
        global_obj.append(obj)


def test_data(data, obj):
    if "splitStart" not in obj or "splitEnd" not in obj:
        obj["splitStart"] = 0
        obj["splitEnd"] = 0 + offset
        return data[obj["splitStart"] : obj["splitEnd"]]
    if obj["splitEnd"] >= len(data) - 1:
        return data[obj["splitStart"] : obj["splitEnd"]]
    obj["splitStart"] = obj["splitStart"] + offset
    obj["splitEnd"] = obj["splitEnd"] + offset
    return data[obj["splitStart"] : obj["splitEnd"]]


def req_data(obj):
    url = f"http://127.0.0.1:8000/ohlcv?name={obj['name']}&frequency={obj['frequency']}&count={obj['count']}&refresh={refresh}"

    r = requests.get(url)
    if r.status_code == 200:
        jsonData = json.loads(r.text)
    else:
        raise ValueError(f"请求出错 {r.status_code} {r.text}")

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


def get_last(data):
    # return data.loc[data.index.stop - 1].copy()
    return data.iloc[len(data) - 1].copy()


def set_last(data, value):
    data.drop(
        ["closeGt", "closeLt", "closeGtDiff", "closeLtDiff"], axis=1, inplace=True
    )
    data.iloc[len(data) - 1] = value


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
    time_array = [["9:00", "11:30"], ["13:30", "15:00"], ["21:00", "23:00"]]
    for i in time_array:
        start = datetime.datetime.strptime(
            str(datetime.datetime.now().date()) + i[0], "%Y-%m-%d%H:%M"
        )
        end = datetime.datetime.strptime(
            str(datetime.datetime.now().date()) + i[1], "%Y-%m-%d%H:%M"
        )
        now_time = datetime.datetime.now()
        if now_time >= start and now_time <= end:
            print("at work time... ", now_time)
            return True
    print("not work time...", now_time)
    return False


def handle_task(data, obj):
    gen_2k(data)
    last = get_last(data)
    if (last["closeGt"] and not last["closeGtDiff"]) | (
        last["closeLt"] and not last["closeLtDiff"]
    ):
        last["level"] = obj["frequency"]
        last["name"] = obj["name"]
        last["mode"] = "lazy"
        last = last.reindex(fields)
        last.drop(["closeGtDiff"], inplace=True)
        last.drop(["closeLtDiff"], inplace=True)

        h = f"{last['mode']} {last['name']} {last['level']} {last['date']}"
        if h not in history:
            if lazy_send:
                send_bot(last)
            history.append(h)
            print("interval 已发送", h)


async def task(obj):
    """
    比如每180s遍历一次螺纹
    """
    while 1:
        run_print(obj, mode="start")
        if isFakeWorkTime or work_time():
            try:
                data = req_data(obj)
                if testUpdate:
                    data = test_data(data, obj)
                    run_print(obj, data=data, mode="test_data")
                else:
                    run_print(obj, data=data, mode="real_data")
                handle_task(data, obj)
                obj["last"] = get_last(data)
                obj["data"] = data
                add_global_obj(obj)
            except ValueError as e:
                print(e)
        print(f"sleep(s): {obj['sleep']}")
        await asyncio.sleep(obj["sleep"])


def current_handle_task(data, current, data_obj, current_obj):
    lazy_close = get_last(data)["close"]
    set_last(data, current)
    current_close = get_last(data)["close"]
    gen_2k(data)
    last = get_last(data)
    if (last["closeGt"] and not last["closeGtDiff"]) | (
        last["closeLt"] and not last["closeLtDiff"]
    ):
        last["level"] = data_obj["frequency"]
        last["name"] = data_obj["name"]
        last["mode"] = "current"
        last["lazy_close"] = lazy_close
        last["current_close"] = current_close
        last = last.reindex(fields)
        last.drop(["closeGtDiff"], inplace=True)
        last.drop(["closeLtDiff"], inplace=True)

        h = f"{last['mode']} {last['name']} {last['level']} {last['date']}"
        if h not in history:
            if current_send:
                send_bot(last)
            history.append(h)
            print("current 已发送", h)


async def current_task(obj):
    """
    比如每6秒获取一次螺纹的6sK线,然后再合并到之前获取的螺纹180sK线里,再进行处理
    """
    while 1:
        run_print(obj, mode="start")
        if isFakeWorkTime or work_time():
            try:
                data = req_data(obj)
                if testUpdate:
                    data = test_data(data, obj)
                    run_print(obj, data=data, mode="test_data")
                else:
                    run_print(obj, data=data, mode="real_data")
                # last = data.loc[data.index.stop - 1].copy()
                last = get_last(data)

                for i in global_obj:
                    if i["name"] == obj["name"] and i["frequency"] != obj["frequency"]:
                        i["current"] = last
                        global_data = i["data"].copy()
                        current_handle_task(
                            global_data,
                            last,
                            i,
                            obj,
                        )

                obj["current"] = last
                obj["last"] = last
                obj["data"] = data
                add_global_obj(obj)
            except ValueError as e:
                print(e)
        print(f"sleep(s): {obj['sleep']}")
        await asyncio.sleep(obj["sleep"])


async def main(array):
    cur_task = (current_task(obj) for obj in current_array) if enable_current else ()
    lazy_task = (task(obj) for obj in array) if enable_lazy else ()
    await asyncio.gather(*lazy_task, *cur_task)


if __name__ == "__main__":

    enable_lazy = True
    enable_current = False

    testUpdate = False  # True的话,启动测试数据,False,启动真实数据
    offset = 10
    refresh = True
    isFakeWorkTime = True  # True的话,可以在work_time时间外运行程序,False,只能在work_time内运行程序
    lazy_send = True
    current_send = True

    import asyncio

    asyncio.run(main(array))
