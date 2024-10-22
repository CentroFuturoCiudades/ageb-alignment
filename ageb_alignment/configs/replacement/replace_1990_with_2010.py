from ageb_alignment.configs.replacement import replace_1990_2000, replace_2000_2010


def build_common_replacement_map(rl_1990_2000: list, rl_2000_2010: list) -> dict:
    sources_1990 = [s if isinstance(s, list) else [s] for s, _ in rl_1990_2000]
    targets_1990 = [t if isinstance(t, list) else [t] for _, t in rl_1990_2000]
    sources_2000 = [s if isinstance(s, list) else [s] for s, _ in rl_2000_2010]
    targets_2000 = [t if isinstance(t, list) else [t] for _, t in rl_2000_2010]

    ST = []

    while len(targets_1990) > 0:
        s_1990 = sources_1990.pop(0)
        t_1990 = targets_1990.pop(0)

        t_1990_set = set(t_1990)
        # Get all 2000 sources that share an element in t
        shared_tlists = [t for t in sources_2000 if len(t_1990_set.intersection(t)) > 0]
        # Convert it to a set of 2000 targets
        t_2000_set = set(sum(shared_tlists, []))
        # For the targets in 2000 not in 1990, add sources and targets lists
        missing = t_2000_set - t_1990_set
        idxs = [
            i for i, t in enumerate(targets_1990) if len(missing.intersection(t)) > 0
        ]
        while len(idxs) > 0:
            for i in idxs:
                t_1990.extend(targets_1990[i])
                s_1990.extend(sources_1990[i])
                del targets_1990[i]
                del sources_1990[i]

            t_1990_set = set(t_1990)
            shared_tlists = [
                t for t in sources_2000 if len(t_1990_set.intersection(t)) > 0
            ]
            t_2000_set = set(sum(shared_tlists, []))
            missing = t_2000_set - t_1990_set
            idxs = [
                i
                for i, t in enumerate(targets_1990)
                if len(missing.intersection(t)) > 0
            ]
        # Get 2000 targets
        r = sum(
            [
                targets_2000[i]
                for i, t in enumerate(sources_2000)
                if len(t_1990_set.intersection(t)) > 0
            ],
            [],
        )
        s = s_1990 if len(s_1990) > 1 else s_1990[0]
        ST.append([s, r])

    return ST


def build_all_replacements(map_1990: dict, map_2000: dict):
    zones_1990 = set(map_1990.keys())
    zones_2000 = set(map_2000.keys())
    common_zones = zones_1990.intersection(zones_2000)
    rep_map = {}
    for zone in common_zones:
        rep_map[zone] = build_common_replacement_map(map_1990[zone], map_2000[zone])
    return rep_map


rep_map = build_all_replacements(replace_1990_2000, replace_2000_2010)
