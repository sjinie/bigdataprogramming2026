# -*- coding: utf-8 -*-
"""Spark 집계 결과 CSV를 차트(PNG)로 시각화.
라벨은 영어/로마자라 한글 폰트가 필요 없다."""
import os
import csv

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt   # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
RES = os.path.join(ROOT, "data", "results")
FIG = os.path.join(RES, "figures")

TEAL = "#0E7C86"
BLUE = "#2980b9"
RED = "#e74c3c"

# 한글 음절 → 로마자(개정 로마자 근사). 폰트 없이 ASCII 라벨을 만들기 위함.
_CHO = ['g', 'kk', 'n', 'd', 'tt', 'r', 'm', 'b', 'pp', 's', 'ss', '',
        'j', 'jj', 'ch', 'k', 't', 'p', 'h']
_JUNG = ['a', 'ae', 'ya', 'yae', 'eo', 'e', 'yeo', 'ye', 'o', 'wa', 'wae',
         'oe', 'yo', 'u', 'wo', 'we', 'wi', 'yu', 'eu', 'ui', 'i']
_JONG = ['', 'g', 'kk', 'gs', 'n', 'nj', 'nh', 'd', 'l', 'lg', 'lm', 'lb',
         'ls', 'lt', 'lp', 'lh', 'm', 'b', 'bs', 's', 'ss', 'ng', 'j', 'ch',
         'k', 't', 'p', 'h']


def romanize(s):
    out = []
    for ch in s:
        c = ord(ch)
        if 0xAC00 <= c <= 0xD7A3:
            i = c - 0xAC00
            out.append(_CHO[i // 588] + _JUNG[(i % 588) // 28] + _JONG[i % 28])
        elif ch in (" ", "-"):
            out.append(" ")
        # 그 외 기호(·, 괄호 등)는 생략
        else:
            out.append(ch if ord(ch) < 128 else "")
    return "".join(out).strip().title()


def read_csv(name):
    p = os.path.join(RES, name)
    if not os.path.isfile(p):
        print("  (skip: {0} not found)".format(name))
        return None
    with open(p, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def save(fig, name):
    if not os.path.isdir(FIG):
        os.makedirs(FIG)
    path = os.path.join(FIG, name)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print("  saved:", path)


def fig_top_regions():
    rows = read_csv("q1_top_regions.csv")
    if not rows:
        return
    rows = rows[:10][::-1]
    names = [romanize(r["region"].split()[-1]) for r in rows]  # 시군구만 로마자
    vals = [float(r["conn_sum"]) for r in rows]
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.barh(names, vals, color=TEAL)
    ax.set_title("Top 10 Hub Districts (sum of Tmap connectivity)")
    ax.set_xlabel("Connectivity (sum)")
    for i, v in enumerate(vals):
        ax.text(v, i, " {0:,.0f}".format(v), va="center", fontsize=9)
    save(fig, "fig1_top_regions.png")


def fig_top_spots():
    rows = read_csv("q1_top_spots.csv")
    if not rows:
        return
    rows = rows[:12][::-1]
    names = [romanize(r["spot"]) for r in rows]
    vals = [float(r["connectivity"]) for r in rows]
    fig, ax = plt.subplots(figsize=(8.5, 6))
    ax.barh(names, vals, color=BLUE)
    ax.set_title("Top 12 Popular Spots (Tmap connectivity)")
    ax.set_xlabel("Connectivity (# of co-visited hub spots)")
    for i, v in enumerate(vals):
        ax.text(v, i, " {0:.0f}".format(v), va="center", fontsize=9)
    save(fig, "fig2_top_spots.png")


def fig_infra_corr():
    rows = read_csv("q2_hub_infra.csv")
    if not rows:
        return
    import numpy as np
    x = np.array([float(r["connectivity"]) for r in rows])
    y = np.array([float(r["infra_total"]) for r in rows])
    fig, ax = plt.subplots(figsize=(7.5, 5.5))
    ax.scatter(x, y, color=TEAL, alpha=0.7, edgecolor="white", s=60)
    if len(x) >= 2:
        m, b = np.polyfit(x, y, 1)
        xs = np.linspace(x.min(), x.max(), 50)
        ax.plot(xs, m * xs + b, color=RED, lw=2, label="regression")
        corr = float(np.corrcoef(x, y)[0, 1])
        ax.text(0.05, 0.92, "Pearson r = {0:.3f}".format(corr),
                transform=ax.transAxes, fontsize=12, fontweight="bold", color=RED)
        ax.legend(loc="lower right")
    ax.set_title("Hub popularity vs. infrastructure within 5km")
    ax.set_xlabel("Connectivity (popularity)")
    ax.set_ylabel("# infrastructure within 5km (stay/food/shop)")
    save(fig, "fig3_infra_corr.png")


def fig_monthly():
    rows = read_csv("q1_monthly.csv")
    if not rows:
        return
    rows = sorted(rows, key=lambda r: r["ym"])
    labels = [r["ym"] for r in rows]              # 이미 ASCII(YYYYMM)
    vals = [float(r["visitors"]) for r in rows]
    fig, ax = plt.subplots(figsize=(9, 4.5))
    ax.plot(labels, vals, marker="o", color=TEAL, lw=2)
    ax.set_title("Monthly Visitors, Non-capital Regions (outsiders + foreigners)")
    ax.set_ylabel("Visitors")
    ax.set_xlabel("Year-Month")
    ax.grid(axis="y", color="#E2E8F0")
    plt.xticks(rotation=45, ha="right", fontsize=8)
    save(fig, "fig4_monthly.png")


def main():
    print("== generating charts ==")
    fig_top_regions()
    fig_top_spots()
    fig_infra_corr()
    fig_monthly()
    print("== done: data/results/figures/ ==")


if __name__ == "__main__":
    main()
