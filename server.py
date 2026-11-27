import asyncio
import json
import math
import os
import secrets
from pathlib import Path
from aiohttp import WSMsgType, web

GAME = Path(__file__).with_name("ashen_keep_v77.html")
HEROES = ("RED", "ORANGE", "YELLOW", "GREEN", "BLUE", "PURPLE", "WHITE")
TROPHIES = {"boar", "troll", "ent", "lich", "commander", "sentinel", "wyrm"}
ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
rooms = {}
UPGRADES = set("speed orangeOrbSpeed area projectileSpeed chainLength orangeFireRate maceSize mirrorDamage projectileCount hp projectileDamage gauntletSize mirrorSpeed damage maceControl mirrorCount maceDamage orangeSpread".split())

def levels(raw):
    if not isinstance(raw,dict):
        return {}
    return {k:int(v) for k,v in raw.items() if k in UPGRADES and type(v) in (int,float) and math.isfinite(v) and 0<=v<=100}



async def index(request):
    return web.FileResponse(GAME, headers={"Cache-Control": "no-cache"})


async def tell(ws, data):
    if not ws.closed:
        try:
            await ws.send_json(data)
        except (ConnectionError, RuntimeError):
            pass


async def broadcast(room, data, except_id=None):
    await asyncio.gather(*(tell(p["ws"], data) for i, p in list(room["members"].items()) if i != except_id))


def roster(room):
    return [{"id": i, "hero": p["hero"]} for i, p in room["members"].items()]


async def lobby(room):
    await broadcast(room, {"t": "lobby", "code": room["code"], "mode": room["mode"], "members": roster(room)})


async def socket(request):
    ws = web.WebSocketResponse(heartbeat=22, max_msg_size=1024*1024, compress=True)
    await ws.prepare(request)
    room, mine = None, None
    try:
        async for message in ws:
            if message.type != WSMsgType.TEXT:
                break
            try:
                data = json.loads(message.data)
            except ValueError:
                break
            if not isinstance(data, dict):
                break
            kind = data.get("t")
            if room is None:
                if kind == "create" and data.get("mode") in ("normal", "raid") and len(rooms) < 500:
                    code = "".join(secrets.choice(ALPHABET) for _ in range(6))
                    while code in rooms:
                        code = "".join(secrets.choice(ALPHABET) for _ in range(6))
                    room = rooms[code] = {"members": {}, "code": code, "mode": data["mode"], "started": False, "done": False}
                    mine = 1
                elif kind == "join" and isinstance(data.get("code"), str):
                    candidate = rooms.get(data["code"].strip().upper())
                    if not candidate or candidate["started"] or len(candidate["members"]) >= 4:
                        await tell(ws, {"t": "error", "message": "코드를 확인해 줘. 진행 중인 방과 4명이 찬 방에는 들어갈 수 없어."})
                        break
                    room = candidate
                    mine = next(i for i in range(2, 5) if i not in room["members"])
                else:
                    await tell(ws, {"t": "error", "message": "방을 열 수 없어. 잠시 뒤 다시 시도해 줘."})
                    break
                room["members"][mine] = {"ws": ws, "hero": None}
                await tell(ws, {"t": "seat", "id": mine})
                await lobby(room)
            elif not room["started"]:
                if kind == "pick":
                    hero = data.get("hero")
                    if hero not in HEROES or any(i != mine and p["hero"] == hero for i, p in room["members"].items()):
                        await tell(ws, {"t": "hint", "message": "친구가 먼저 선택했어. 다른 챔피언을 골라 줘."})
                    else:
                        room["members"][mine]["hero"] = hero
                        room["members"][mine]["up"] = levels(data.get("up"))
                        await lobby(room)
                elif kind == "start" and mine == 1:
                    if len(room["members"]) < 2 or any(not p["hero"] for p in room["members"].values()):
                        await tell(ws, {"t": "hint", "message": "2~4명이 모이고 모두 챔피언을 골라야 시작할 수 있어."})
                    else:
                        room["started"] = True
                        room["heroes"] = {p["id"]:p["hero"] for p in roster(room)}
                        await broadcast(room, {"t": "begin", "mode": room["mode"], "members": [{**p,"up":room["members"][p["id"]].get("up",{})} for p in roster(room)]})
            elif mine == 1 and kind == "frame" and isinstance(data.get("world"), dict):
                await asyncio.gather(*(p["ws"].send_str(message.data) for i,p in list(room["members"].items()) if i != 1 and not p["ws"].closed),return_exceptions=True)
            elif mine != 1 and kind == "received" and room["done"]:
                room["pending"].discard(mine)
                if not room["pending"]:
                    room["delivered"].set()
            elif mine != 1 and kind == "input" and not room["done"]:
                axes = [data.get(k) for k in ("x", "y")]
                if all(type(n) in (float,int) and abs(n)<=1.001 and math.isfinite(n) for n in axes):
                    await tell(room["members"][1]["ws"], {"t":"input","id":mine,"x":axes[0],"y":axes[1]})
            elif mine != 1 and kind == "choice" and type(data.get("token")) is int and type(data.get("index")) is int and 0<=data["index"]<3:
                await tell(room["members"][1]["ws"], {"t":"choice","id":mine,"token":data["token"],"index":data["index"]})
            elif mine == 1 and kind == "result" and not room["done"]:
                room["done"] = True
                room["pending"] = set(room["members"]) - {1}
                room["delivered"] = asyncio.Event()
                await broadcast(room, {"t":"result","win":data.get("win") is True,
                                       "stats":battle_stats(data.get("stats"),room["heroes"]),
                                       "coins":bounded(data.get("coins")),"kills":bounded(data.get("kills")),
                                       "trophies":[v for v in (data.get("trophies") or [])[:7] if isinstance(v,str) and v in TROPHIES]}, except_id=1)
                try:
                    await asyncio.wait_for(room["delivered"].wait(),timeout=4)
                except asyncio.TimeoutError:
                    pass
                await tell(ws, {"t":"done"})
                break
    except (ConnectionError, asyncio.CancelledError):
        pass
    finally:
        if room and rooms.get(room["code"]) is room:
            room["members"].pop(mine,None)
            if room["done"] and "pending" in room:
                room["pending"].discard(mine)
                if not room["pending"]:
                    room["delivered"].set()
            if mine==1:
                rooms.pop(room["code"],None)
                if not room["done"]:
                    await broadcast(room,{"t":"left","message":"방장이 연결을 종료했어."})
                for p in list(room["members"].values()):
                    await p["ws"].close()
            elif not room["started"]:
                await lobby(room)
            elif not room["done"]:
                await broadcast(room,{"t":"depart","id":mine})
        await ws.close()
    return ws


def battle_stats(raw, heroes):
    entries = {}
    if isinstance(raw, list):
        for row in raw[:4]:
            if isinstance(row, dict) and type(row.get("seat")) is int and row["seat"] in heroes:
                entries[row["seat"]] = row
    result = []
    for seat, hero in sorted(heroes.items()):
        row = entries.get(seat, {})
        counters = {k: min(2**53-1, max(0, row[k])) if type(row.get(k)) is int else 0
                    for k in ("dealt", "taken", "deaths")}
        result.append({"seat":seat, "hero":hero, **counters})
    return result


def bounded(value):
    return min(10**7, max(0,int(value))) if type(value) in (float,int) and math.isfinite(value) else 0


def app():
    application=web.Application()
    application.router.add_get("/",index)
    application.router.add_get("/ashen_keep_v77.html",index)
    application.router.add_get("/coop",socket)
    return application


if __name__=="__main__":
    web.run_app(app(),host="0.0.0.0",port=int(os.getenv("PORT","10000")))
