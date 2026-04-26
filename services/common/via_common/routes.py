from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Route:
    route_id: str
    trains: set[str]


ROUTES: list[Route] = [
    Route(
        route_id="toronto-kingston-montreal",
        trains={"60", "61", "62", "63", "64", "65", "66", "67", "68", "69", "668", "669"},
    ),
    Route(
        route_id="toronto-kingston-ottawa",
        trains={"40", "41", "42", "44", "45", "46", "47", "48", "50", "52", "53", "54", "55", "59", "643", "645"},
    ),
    Route(
        route_id="ottawa-montreal-quebec-city",
        trains={"20", "22", "24", "26", "28", "29", "31", "33", "35", "37", "38", "39", "622", "633"},
    ),
    Route(route_id="toronto-niagara-new-york", trains={"97", "98"}),
    Route(route_id="sarnia-london-toronto", trains={"84", "87"}),
    Route(route_id="windsor-london-toronto", trains={"70", "71", "72", "73", "75", "76", "78", "79"}),
]


def route_id_for_train(train_number: str) -> str:
    t = str(train_number)
    for r in ROUTES:
        if t in r.trains:
            return r.route_id
    return "unknown"


def route_hash(route_id: str) -> int:
    return abs(hash(route_id)) % 1000
