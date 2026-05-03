"""Generate realistic synthetic client workbooks for Sofia PVRP validation.

The generator intentionally creates clustered, sales-rep-owned territories.
It is test data for the route-first PVRP pipeline, not a replacement for the
solver or candidate route logic.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd


REQUIRED_COLUMNS = [
    "client_id",
    "client_name",
    "sales_rep",
    "lat",
    "lon",
    "visit_frequency",
    "fixed_weekday",
    "forbidden_weekdays",
    "preferred_weekdays",
    "cluster_manual",
    "notes",
]

WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]


@dataclass(frozen=True)
class Zone:
    name: str
    lat: float
    lon: float
    sigma_lat: float
    sigma_lon: float
    urban_score: float
    peripheral_score: float


ZONES: dict[str, Zone] = {
    "Center": Zone("Center", 42.6977, 23.3219, 0.010, 0.014, 1.00, 0.05),
    "Lozenets": Zone("Lozenets", 42.6766, 23.3204, 0.010, 0.014, 0.95, 0.05),
    "Ivan Vazov": Zone("Ivan Vazov", 42.6805, 23.3068, 0.008, 0.012, 0.90, 0.05),
    "Studentski Grad": Zone("Studentski Grad", 42.6508, 23.3482, 0.010, 0.014, 0.90, 0.05),
    "Mladost": Zone("Mladost", 42.6448, 23.3784, 0.012, 0.016, 0.90, 0.06),
    "Druzhba": Zone("Druzhba", 42.6602, 23.4067, 0.012, 0.016, 0.82, 0.08),
    "Gorublyane": Zone("Gorublyane", 42.6320, 23.4095, 0.010, 0.015, 0.55, 0.30),
    "Slatina": Zone("Slatina", 42.6827, 23.3675, 0.010, 0.014, 0.86, 0.06),
    "Geo Milev": Zone("Geo Milev", 42.6838, 23.3542, 0.008, 0.012, 0.88, 0.05),
    "Reduta": Zone("Reduta", 42.6904, 23.3568, 0.007, 0.010, 0.78, 0.08),
    "Hadzhi Dimitar": Zone("Hadzhi Dimitar", 42.7104, 23.3447, 0.009, 0.012, 0.80, 0.08),
    "Poduyane": Zone("Poduyane", 42.7055, 23.3527, 0.009, 0.012, 0.80, 0.08),
    "Oborishte": Zone("Oborishte", 42.7003, 23.3353, 0.008, 0.011, 0.88, 0.04),
    "Nadezhda": Zone("Nadezhda", 42.7302, 23.3089, 0.012, 0.015, 0.78, 0.12),
    "Vrabnitsa": Zone("Vrabnitsa", 42.7414, 23.2861, 0.011, 0.015, 0.60, 0.25),
    "Obelya": Zone("Obelya", 42.7467, 23.2748, 0.011, 0.015, 0.62, 0.25),
    "Lyulin": Zone("Lyulin", 42.7170, 23.2565, 0.014, 0.018, 0.78, 0.14),
    "Krasna Polyana": Zone("Krasna Polyana", 42.6965, 23.2778, 0.011, 0.015, 0.72, 0.12),
    "Ovcha Kupel": Zone("Ovcha Kupel", 42.6783, 23.2563, 0.013, 0.016, 0.70, 0.18),
    "Krasno Selo": Zone("Krasno Selo", 42.6844, 23.2894, 0.010, 0.014, 0.82, 0.08),
    "Knyazhevo": Zone("Knyazhevo", 42.6574, 23.2464, 0.011, 0.014, 0.50, 0.35),
    "Boyana": Zone("Boyana", 42.6442, 23.2662, 0.010, 0.014, 0.45, 0.42),
    "Dragalevtsi": Zone("Dragalevtsi", 42.6281, 23.3091, 0.010, 0.014, 0.46, 0.42),
    "Simeonovo": Zone("Simeonovo", 42.6206, 23.3360, 0.010, 0.014, 0.45, 0.42),
    "Bozhurishte": Zone("Bozhurishte", 42.7597, 23.1974, 0.014, 0.018, 0.22, 0.80),
    "Kostinbrod": Zone("Kostinbrod", 42.8155, 23.2141, 0.016, 0.022, 0.18, 0.85),
    "Bankya": Zone("Bankya", 42.7063, 23.1436, 0.014, 0.020, 0.30, 0.75),
    "Novi Iskar": Zone("Novi Iskar", 42.8028, 23.3503, 0.016, 0.022, 0.25, 0.80),
    "Elin Pelin": Zone("Elin Pelin", 42.6669, 23.6016, 0.018, 0.024, 0.22, 0.85),
    "Kazichene": Zone("Kazichene", 42.6581, 23.4589, 0.014, 0.018, 0.32, 0.70),
    "German": Zone("German", 42.6209, 23.4113, 0.012, 0.016, 0.38, 0.58),
    "Lozen": Zone("Lozen", 42.6008, 23.4852, 0.014, 0.020, 0.28, 0.75),
    "Bistritsa": Zone("Bistritsa", 42.5926, 23.3608, 0.012, 0.018, 0.35, 0.65),
    "Pancharevo": Zone("Pancharevo", 42.6044, 23.4050, 0.014, 0.020, 0.30, 0.72),
    "Samokov": Zone("Samokov", 42.3370, 23.5528, 0.020, 0.028, 0.12, 0.95),
    "Ihtiman": Zone("Ihtiman", 42.4333, 23.8167, 0.020, 0.028, 0.12, 0.95),
    "Vakarel": Zone("Vakarel", 42.5550, 23.7115, 0.018, 0.026, 0.10, 0.95),
    "Svoge": Zone("Svoge", 42.9667, 23.3500, 0.020, 0.026, 0.12, 0.95),
    "Pernik": Zone("Pernik", 42.6052, 23.0378, 0.020, 0.028, 0.20, 0.90),
    "Airport": Zone("Airport", 42.6964, 23.4114, 0.012, 0.016, 0.50, 0.35),
    "Darvenitsa": Zone("Darvenitsa", 42.6532, 23.3592, 0.009, 0.012, 0.82, 0.08),
}


TERRITORIES: list[list[str]] = [
    ["Lyulin", "Obelya", "Bankya", "Bozhurishte"],
    ["Mladost", "Druzhba", "Gorublyane", "Kazichene"],
    ["Center", "Lozenets", "Ivan Vazov", "Studentski Grad"],
    ["Nadezhda", "Vrabnitsa", "Novi Iskar", "Kostinbrod"],
    ["Slatina", "Geo Milev", "Reduta", "Hadzhi Dimitar"],
    ["Elin Pelin", "Kazichene", "German", "Lozen"],
    ["Bistritsa", "Pancharevo", "Boyana", "Simeonovo"],
    ["Ovcha Kupel", "Krasno Selo", "Knyazhevo", "Bankya"],
    ["Samokov", "Bistritsa", "Pancharevo", "Lozen"],
    ["Ihtiman", "Elin Pelin", "Vakarel", "Lozen"],
    ["Svoge", "Novi Iskar", "Kostinbrod", "Nadezhda"],
    ["Pernik", "Bankya", "Knyazhevo", "Lyulin"],
    ["Center", "Oborishte", "Poduyane", "Hadzhi Dimitar"],
    ["Studentski Grad", "Mladost", "Darvenitsa", "Lozenets"],
    ["Druzhba", "Slatina", "Kazichene", "Airport"],
    ["Boyana", "Dragalevtsi", "Simeonovo", "Bistritsa"],
    ["Lyulin", "Krasna Polyana", "Ovcha Kupel", "Krasno Selo"],
    ["Gorublyane", "German", "Lozen", "Elin Pelin"],
]


def _scenario_defaults(scenario: str) -> tuple[int, int]:
    normalized = scenario.lower()
    if normalized in {"small", "small_feasible"}:
        return 2, 80
    if normalized in {"medium", "medium_feasible"}:
        return 5, 500
    if normalized in {"full", "full_1800", "full_1800_feasible"}:
        return 18, 1800
    if normalized in {"infeasible", "infeasible_capacity", "capacity_infeasible"}:
        return 18, 1800
    if normalized in {"bad", "bad_coordinates"}:
        return 3, 60
    raise ValueError(f"Unknown scenario: {scenario}")


def _counts_by_rep(n_sales_reps: int, n_clients: int) -> list[int]:
    base = n_clients // n_sales_reps
    remainder = n_clients % n_sales_reps
    return [base + (1 if i < remainder else 0) for i in range(n_sales_reps)]


def _balanced_counts(total: int, weights: dict[int, float]) -> dict[int, int]:
    raw = {freq: total * share for freq, share in weights.items()}
    counts = {freq: int(np.floor(value)) for freq, value in raw.items()}
    missing = total - sum(counts.values())
    order = sorted(weights, key=lambda freq: raw[freq] - counts[freq], reverse=True)
    for freq in order[:missing]:
        counts[freq] += 1
    return counts


def _frequency_counts_for_rep(
    scenario: str,
    rep_index: int,
    rep_count: int,
    n_sales_reps: int,
    n_clients: int,
    urban_rank: int,
    peripheral_rank: int,
) -> dict[int, int]:
    normalized = scenario.lower()
    if normalized in {"infeasible", "infeasible_capacity", "capacity_infeasible"}:
        return _balanced_counts(rep_count, {2: 0.08, 4: 0.20, 8: 0.72})

    if normalized in {"full", "full_1800", "full_1800_feasible"} and n_sales_reps == 18 and n_clients == 1800:
        freq8 = 12 if urban_rank < 2 else 11
        freq2 = 39 if peripheral_rank < 16 else 38
        freq4 = rep_count - freq2 - freq8
        return {2: freq2, 4: freq4, 8: freq8}

    if normalized in {"medium", "medium_feasible"}:
        return _balanced_counts(rep_count, {2: 0.40, 4: 0.50, 8: 0.10})
    if normalized in {"small", "small_feasible", "bad", "bad_coordinates"}:
        return _balanced_counts(rep_count, {2: 0.36, 4: 0.50, 8: 0.14})
    return _balanced_counts(rep_count, {2: 0.39, 4: 0.50, 8: 0.11})


def _territory_scores(n_sales_reps: int) -> tuple[dict[int, int], dict[int, int]]:
    rep_scores = []
    for idx in range(n_sales_reps):
        territory = TERRITORIES[idx % len(TERRITORIES)]
        urban = float(np.mean([ZONES[z].urban_score for z in territory]))
        peripheral = float(np.mean([ZONES[z].peripheral_score for z in territory]))
        rep_scores.append((idx, urban, peripheral))
    urban_order = {idx: rank for rank, (idx, _, _) in enumerate(sorted(rep_scores, key=lambda item: item[1], reverse=True))}
    peripheral_order = {idx: rank for rank, (idx, _, _) in enumerate(sorted(rep_scores, key=lambda item: item[2], reverse=True))}
    return urban_order, peripheral_order


def _choose_zone(rng: np.random.Generator, territory: list[str], noise_probability: float) -> tuple[Zone, bool]:
    all_zone_names = list(ZONES)
    if rng.random() < noise_probability:
        outside = [name for name in all_zone_names if name not in territory]
        return ZONES[str(rng.choice(outside))], True

    scores = np.array([0.60 + ZONES[name].urban_score + ZONES[name].peripheral_score * 0.45 for name in territory])
    scores = scores / scores.sum()
    zone_name = str(rng.choice(territory, p=scores))
    return ZONES[zone_name], False


def _assign_frequencies(rep_df: pd.DataFrame, counts: dict[int, int], rng: np.random.Generator) -> pd.Series:
    scores = rep_df.copy()
    scores["urban_score"] = scores["cluster_manual"].map(lambda z: ZONES[str(z)].urban_score)
    scores["peripheral_score"] = scores["cluster_manual"].map(lambda z: ZONES[str(z)].peripheral_score)
    scores["urban_rank_score"] = scores["urban_score"] + rng.normal(0, 0.12, len(scores))
    scores["peripheral_rank_score"] = scores["peripheral_score"] + rng.normal(0, 0.12, len(scores))

    frequencies = pd.Series(4, index=rep_df.index, dtype=int)

    freq8_count = min(counts.get(8, 0), len(scores))
    freq8_indices = scores.sort_values("urban_rank_score", ascending=False).head(freq8_count).index
    frequencies.loc[freq8_indices] = 8

    remaining = scores.drop(index=freq8_indices)
    freq2_count = min(counts.get(2, 0), len(remaining))
    freq2_indices = remaining.sort_values("peripheral_rank_score", ascending=False).head(freq2_count).index
    frequencies.loc[freq2_indices] = 2
    return frequencies


def _weekday_preferences(freq: int, rng: np.random.Generator) -> tuple[str, str, str]:
    fixed = ""
    forbidden = ""
    preferred = ""
    if freq in {2, 4} and rng.random() < 0.015:
        fixed = str(rng.choice(WEEKDAYS))
    if rng.random() < 0.035:
        forbidden = str(rng.choice(WEEKDAYS))
    if rng.random() < 0.085:
        preferred_days = sorted(rng.choice(WEEKDAYS, size=2, replace=False).tolist(), key=WEEKDAYS.index)
        preferred = ",".join(preferred_days)
    return fixed, forbidden, preferred


def _inject_bad_rows(df: pd.DataFrame) -> pd.DataFrame:
    bad = df.copy()
    if len(bad) < 8:
        raise ValueError("bad_coordinates scenario needs at least 8 clients")
    bad.loc[0, "lat"] = np.nan
    bad.loc[1, "lon"] = np.nan
    bad.loc[2, ["lat", "lon"]] = [bad.loc[2, "lon"], bad.loc[2, "lat"]]
    bad.loc[3, ["lat", "lon"]] = [95.0, 230.0]
    bad.loc[4, "client_id"] = bad.loc[5, "client_id"]
    bad.loc[6, "visit_frequency"] = 6
    bad.loc[7, "notes"] = "synthetic bad row: control case"
    return bad


def _generate_small_feasible_controlled(
    n_sales_reps: int,
    n_clients: int,
    rng: np.random.Generator,
) -> pd.DataFrame:
    """Create an exact-cover-friendly small smoke dataset.

    Every rep has clients split into compact 20-ish manual zones with frequency
    4, so the route-first master can reuse the same selected zone routes once
    per week under the production 20-client route profile.
    """
    rows: list[dict[str, object]] = []
    client_number = 1
    for rep_idx, rep_client_count in enumerate(_counts_by_rep(n_sales_reps, n_clients)):
        sales_rep = f"TP_{rep_idx + 1:02d}"
        territory = TERRITORIES[rep_idx % len(TERRITORIES)][:2]
        zone_counts = _counts_by_rep(len(territory), rep_client_count)
        for zone_name, zone_count in zip(territory, zone_counts):
            zone = ZONES[zone_name]
            for _ in range(zone_count):
                lat = zone.lat + float(rng.normal(0, zone.sigma_lat * 0.45))
                lon = zone.lon + float(rng.normal(0, zone.sigma_lon * 0.45))
                rows.append(
                    {
                        "client_id": f"C{client_number:05d}",
                        "client_name": f"Client {client_number:05d}",
                        "sales_rep": sales_rep,
                        "lat": round(lat, 6),
                        "lon": round(lon, 6),
                        "visit_frequency": 4,
                        "fixed_weekday": "",
                        "forbidden_weekdays": "",
                        "preferred_weekdays": "",
                        "cluster_manual": zone.name,
                        "notes": f"synthetic-small-feasible;zone={zone.name};territory={sales_rep};noise=0",
                    }
                )
                client_number += 1
    return pd.DataFrame(rows, columns=REQUIRED_COLUMNS)


def _write_workbook(df: pd.DataFrame, output_path: str | Path) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Clients", index=False)


def generate_synthetic_clients(
    n_sales_reps: int,
    n_clients: int,
    scenario: str,
    output_path: str,
    random_seed: int = 42,
) -> pd.DataFrame:
    """Generate a clustered Sofia/Sofia-region synthetic client workbook."""
    if n_sales_reps <= 0:
        raise ValueError("n_sales_reps must be positive")
    if n_clients < n_sales_reps:
        raise ValueError("n_clients must be at least n_sales_reps")

    rng = np.random.default_rng(random_seed)
    normalized = scenario.lower()
    if normalized in {"small", "small_feasible"}:
        df = _generate_small_feasible_controlled(n_sales_reps, n_clients, rng)
        if output_path:
            _write_workbook(df, output_path)
        return df

    counts = _counts_by_rep(n_sales_reps, n_clients)
    urban_rank, peripheral_rank = _territory_scores(n_sales_reps)
    rows: list[dict[str, object]] = []
    client_number = 1

    for rep_idx, rep_client_count in enumerate(counts):
        sales_rep = f"TP_{rep_idx + 1:02d}"
        territory = TERRITORIES[rep_idx % len(TERRITORIES)]
        outer_share = float(np.mean([ZONES[z].peripheral_score for z in territory]))
        noise_probability = 0.035 + min(0.075, outer_share * 0.06)

        rep_rows: list[dict[str, object]] = []
        for _ in range(rep_client_count):
            zone, is_noise = _choose_zone(rng, territory, noise_probability)
            lat = zone.lat + float(rng.normal(0, zone.sigma_lat * (1.45 if is_noise else 1.0)))
            lon = zone.lon + float(rng.normal(0, zone.sigma_lon * (1.45 if is_noise else 1.0)))
            rep_rows.append(
                {
                    "client_id": f"C{client_number:05d}",
                    "client_name": f"Client {client_number:05d}",
                    "sales_rep": sales_rep,
                    "lat": round(lat, 6),
                    "lon": round(lon, 6),
                    "visit_frequency": 4,
                    "fixed_weekday": "",
                    "forbidden_weekdays": "",
                    "preferred_weekdays": "",
                    "cluster_manual": zone.name,
                    "notes": f"synthetic;zone={zone.name};territory={sales_rep};noise={int(is_noise)}",
                }
            )
            client_number += 1

        rep_df = pd.DataFrame(rep_rows)
        freq_counts = _frequency_counts_for_rep(
            normalized,
            rep_idx,
            rep_client_count,
            n_sales_reps,
            n_clients,
            urban_rank[rep_idx],
            peripheral_rank[rep_idx],
        )
        rep_df["visit_frequency"] = _assign_frequencies(rep_df, freq_counts, rng)
        for idx, freq in rep_df["visit_frequency"].items():
            fixed, forbidden, preferred = _weekday_preferences(int(freq), rng)
            rep_df.loc[idx, "fixed_weekday"] = fixed
            rep_df.loc[idx, "forbidden_weekdays"] = forbidden
            rep_df.loc[idx, "preferred_weekdays"] = preferred
        rows.extend(rep_df.to_dict("records"))

    df = pd.DataFrame(rows, columns=REQUIRED_COLUMNS)
    if normalized in {"bad", "bad_coordinates"}:
        df = _inject_bad_rows(df)

    if output_path:
        _write_workbook(df, output_path)
    return df


def generate_standard_datasets(output_dir: str | Path = "data", random_seed: int = 42) -> list[Path]:
    """Generate all standard validation workbooks under output_dir."""
    output = Path(output_dir)
    jobs: list[tuple[str, int, int, str]] = [
        ("small_feasible", 2, 80, "synthetic_small_feasible.xlsx"),
        ("medium_feasible", 5, 500, "synthetic_medium_feasible.xlsx"),
        ("full_1800", 18, 1800, "synthetic_1800_sofia.xlsx"),
        ("infeasible_capacity", 18, 1800, "synthetic_infeasible_capacity.xlsx"),
        ("bad_coordinates", 3, 60, "synthetic_bad_coordinates.xlsx"),
    ]
    paths: list[Path] = []
    for scenario, reps, clients, filename in jobs:
        path = output / filename
        generate_synthetic_clients(reps, clients, scenario, str(path), random_seed=random_seed)
        paths.append(path)
    return paths


def _parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic Sofia client Excel files.")
    parser.add_argument(
        "--scenario",
        default="full_1800",
        choices=[
            "small_feasible",
            "medium_feasible",
            "full_1800",
            "infeasible_capacity",
            "bad_coordinates",
            "all",
        ],
        help="Synthetic data scenario to generate.",
    )
    parser.add_argument("--output", default="data/synthetic_1800_sofia.xlsx", help="Output .xlsx path.")
    parser.add_argument("--n-sales-reps", type=int, help="Override sales rep count.")
    parser.add_argument("--n-clients", type=int, help="Override client count.")
    parser.add_argument("--random-seed", type=int, default=42, help="Deterministic random seed.")
    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.scenario == "all":
        paths = generate_standard_datasets(Path(args.output).parent if args.output else "data", args.random_seed)
        for path in paths:
            print(f"Generated {path}")
        return 0

    default_reps, default_clients = _scenario_defaults(args.scenario)
    n_sales_reps = args.n_sales_reps or default_reps
    n_clients = args.n_clients or default_clients
    df = generate_synthetic_clients(n_sales_reps, n_clients, args.scenario, args.output, args.random_seed)
    visits = int(pd.to_numeric(df["visit_frequency"], errors="coerce").fillna(0).sum())
    print(
        f"Generated {len(df)} clients, {df['sales_rep'].nunique()} sales reps, "
        f"{visits} required visits -> {args.output}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
